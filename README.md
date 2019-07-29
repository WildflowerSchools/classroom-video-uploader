# Camera-node

All software needed for a camera node in an environment. Includes pieces for both capture and delivery as well as proxy node that handles centralized processing of video, upload, and queuing in case of slow or down connectivity.

## Proxy

Flask service that receives data from all other camera nodes and feeds and internal queue and that sends data to honeycomb and optionally processes videos to pull out frames that are also sent to honeycomb.

## Capture

Service that runs on nodes that have cameras. Captures video clips in short segments and sends them to their internal queue where workers send the to the proxy.

## Workers

Celery service that performs tasks from the internal queue. Capture work is processed and sent to the proxy, unless it is a the proxy-node in which case it forwards the work to another task. Video segments are processes and sent to honeycomb. Processing includes tagging etc. Once uploaded the next set of processing happens, key frames are extracted at those are sent to honeycomb as child datapoint objects of the video.

If running on more capable hardware the keyframes could be evaluated for things like pose detection or object tracking. This is not planned yet but isn't out of scope specifically.

## Radio-Monitor

A python service that connects to a network of DWM1001 modules over BLE to collect data. That data is queued to be sent to honeycomb. It is expected that this service runs on the proxy node.
