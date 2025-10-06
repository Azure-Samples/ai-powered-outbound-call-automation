from quart import Quart, Response, request, json, websocket
from azure.eventgrid import EventGridEvent, SystemEventNames
from urllib.parse import urlencode, urlparse, urlunparse
from logging import INFO
from azure.communication.callautomation import (
    MediaStreamingOptions,
    AudioFormat,
    MediaStreamingContentType,
    MediaStreamingAudioChannelType,
    StreamingTransportType,
    PhoneNumberIdentifier,
    CommunicationUserIdentifier
    )
from azure.communication.callautomation.aio import (
    CallAutomationClient
    )
import uuid

from azureOpenAIService import OpenAIRTHandler

# Your ACS resource connection string
ACS_CONNECTION_STRING = ""

# Callback events URI to handle callback events.
CALLBACK_URI_HOST = ""
CALLBACK_EVENTS_URI = CALLBACK_URI_HOST + "/api/callbacks"

acs_client = CallAutomationClient.from_connection_string(ACS_CONNECTION_STRING)
app = Quart(__name__)

# Place an outbound call POST endpoint is used by the frontend system to place an outbound call.
@app.route("/api/placeCall", methods=["POST"])
async def place_call():
    """
    Body:
      {
        "to": "+15551234567" | "8:acs:identity",
        "operationContext": "optional-string",
        "callerId": "+15557654321"            # optional (PSTN presentation in ACS)
      }
    """
    body = await request.get_json()
    target_raw = body.get("to")
    if not target_raw:
        return Response(response=json.dumps({"error": "'to' required"}), status=400)

    op_ctx = body.get("operationContext", "outboundCall")
    caller_id_number = body.get("callerId")  # for PSTN presentation (must be an acquired number)

    # Build CommunicationIdentifier for target
    if target_raw.startswith("+"):
        target_identifier = PhoneNumberIdentifier(target_raw)
    elif target_raw.startswith("8:acs:"):
        target_identifier = CommunicationUserIdentifier(target_raw)
    else:
        return Response(response=json.dumps({"error": "Unsupported 'to' format"}), status=400)

    guid = uuid.uuid4()
    query_parameters = urlencode({"direction": "outbound", "to": target_raw})
    callback_uri = f"{CALLBACK_EVENTS_URI}/{guid}?{query_parameters}"

    parsed_url = urlparse(CALLBACK_EVENTS_URI)
    websocket_url = urlunparse(("wss", parsed_url.netloc, "/ws", "", "", ""))

    media_streaming_options = MediaStreamingOptions(
        transport_url=websocket_url,
        transport_type=StreamingTransportType.WEBSOCKET,
        content_type=MediaStreamingContentType.AUDIO,
        audio_channel_type=MediaStreamingAudioChannelType.MIXED,
        start_media_streaming=True,
        enable_bidirectional=True,
        audio_format=AudioFormat.PCM24_K_MONO
    )

    create_kwargs = {
        "target_participant": target_identifier,
        "callback_url": callback_uri,
        "media_streaming": media_streaming_options,
        "operation_context": op_ctx
    }
    if caller_id_number:
        create_kwargs["source_caller_id_number"] = PhoneNumberIdentifier(caller_id_number)

    #ACS Call Automation needs a transport url which is websocket endpoint to stream the media. And the callback url to send the call related events.
    create_call_result = await acs_client.create_call(**create_kwargs)
    call_connection_id = create_call_result.call_connection_id
    app.logger.info("Started outbound call. CallConnectionId=%s", call_connection_id)

    return Response(
        response=json.dumps({
            "callConnectionId": call_connection_id,
            "callbackUrl": callback_uri,
            "websocketUrl": websocket_url,
            "operationContext": op_ctx
        }),
        status=202
    )


@app.route("/api/incomingCall",  methods=['POST'])
async def incoming_call_handler():
    app.logger.info("incoming event data")
    for event_dict in await request.json:
            event = EventGridEvent.from_dict(event_dict)
            app.logger.info("incoming event data --> %s", event.data)
            if event.event_type == SystemEventNames.EventGridSubscriptionValidationEventName:
                app.logger.info("Validating subscription")
                validation_code = event.data['validationCode']
                validation_response = {'validationResponse': validation_code}
                return Response(response=json.dumps(validation_response), status=200)
            elif event.event_type =="Microsoft.Communication.IncomingCall":
                app.logger.info("Incoming call received: data=%s", 
                                event.data)  
                if event.data['from']['kind'] =="phoneNumber":
                    caller_id =  event.data['from']["phoneNumber"]["value"]
                else :
                    caller_id =  event.data['from']['rawId'] 
                app.logger.info("incoming call handler caller id: %s",
                                caller_id)
                incoming_call_context=event.data['incomingCallContext']
                guid =uuid.uuid4()
                query_parameters = urlencode({"callerId": caller_id})
                callback_uri = f"{CALLBACK_EVENTS_URI}/{guid}?{query_parameters}"
                
                parsed_url = urlparse(CALLBACK_EVENTS_URI)
                websocket_url = urlunparse(('wss',parsed_url.netloc,'/ws','', '', ''))

                app.logger.info("callback url: %s",  callback_uri)
                app.logger.info("websocket url: %s",  websocket_url)

                media_streaming_options = MediaStreamingOptions(
                        transport_url=websocket_url,
                        transport_type=StreamingTransportType.WEBSOCKET,
                        content_type=MediaStreamingContentType.AUDIO,
                        audio_channel_type=MediaStreamingAudioChannelType.MIXED,
                        start_media_streaming=True,
                        enable_bidirectional=True,
                        audio_format=AudioFormat.PCM24_K_MONO)
                
                answer_call_result = await acs_client.answer_call(incoming_call_context=incoming_call_context,
                                                            operation_context="incomingCall",
                                                            callback_url=callback_uri, 
                                                            media_streaming=media_streaming_options)
                app.logger.info("Answered call for connection id: %s",
                                answer_call_result.call_connection_id)
            return Response(status=200)

# Callback endpoint to receive call related events. This endpoint will be called by ACS Call Automation service. It can be used to setup a counter for active calls. 
@app.route('/api/callbacks/<contextId>', methods=['POST'])
async def callbacks(contextId):
     for event in await request.json:
        # Parsing callback events
        global call_connection_id
        event_data = event['data']
        call_connection_id = event_data["callConnectionId"]
        app.logger.info(f"Received Event:-> {event['type']}, Correlation Id:-> {event_data['correlationId']}, CallConnectionId:-> {call_connection_id}")
        if event['type'] == "Microsoft.Communication.CallConnected":
            call_connection_properties = await acs_client.get_call_connection(call_connection_id).get_call_properties()
            media_streaming_subscription = call_connection_properties.media_streaming_subscription
            app.logger.info(f"MediaStreamingSubscription:--> {media_streaming_subscription}")
            app.logger.info(f"Received CallConnected event for connection id: {call_connection_id}")
            app.logger.info("CORRELATION ID:--> %s", event_data["correlationId"])
            app.logger.info("CALL CONNECTION ID:--> %s", event_data["callConnectionId"])
        elif event['type'] == "Microsoft.Communication.MediaStreamingStarted":
            app.logger.info(f"Media streaming content type:--> {event_data['mediaStreamingUpdate']['contentType']}")
            app.logger.info(f"Media streaming status:--> {event_data['mediaStreamingUpdate']['mediaStreamingStatus']}")
            app.logger.info(f"Media streaming status details:--> {event_data['mediaStreamingUpdate']['mediaStreamingStatusDetails']}")
        elif event['type'] == "Microsoft.Communication.MediaStreamingStopped":
            app.logger.info(f"Media streaming content type:--> {event_data['mediaStreamingUpdate']['contentType']}")
            app.logger.info(f"Media streaming status:--> {event_data['mediaStreamingUpdate']['mediaStreamingStatus']}")
            app.logger.info(f"Media streaming status details:--> {event_data['mediaStreamingUpdate']['mediaStreamingStatusDetails']}")
        elif event['type'] == "Microsoft.Communication.MediaStreamingFailed":
            app.logger.info(f"Code:->{event_data['resultInformation']['code']}, Subcode:-> {event_data['resultInformation']['subCode']}")
            app.logger.info(f"Message:->{event_data['resultInformation']['message']}")
        elif event['type'] == "Microsoft.Communication.CallDisconnected":
            pass
     return Response(status=200)

# WebSocket. Two separate websocket endpoints can be created for different AOAI regions.
@app.websocket('/ws')
async def ws():
    handler = OpenAIRTHandler()
    print("Client connected to WebSocket")
    await handler.init_incoming_websocket(websocket)
    await handler.start_client()
    while websocket:
        try:
            # Receive data from the client
            data = await websocket.receive()
            await handler.acs_to_oai(data)
            await handler.send_welcome()
        except Exception as e:
            print(f"WebSocket connection closed: {e}")
            break

@app.route('/')
def home():
    return 'Hello ACS CallAutomation!'

if __name__ == '__main__':
    app.logger.setLevel(INFO)
    app.run(port=8000)
