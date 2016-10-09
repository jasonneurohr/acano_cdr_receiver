from http.server import BaseHTTPRequestHandler, HTTPServer
import xmltodict
import getopt
import sys
import os
import datetime

log_file = ""


class S(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write("<html><body><h1>hello</h1></body></html>")

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        """
        Handles POST request from the Acano Call Bridge and parses the CDR data into the parser function
        :return:
        """
        self._set_headers()
        # For future use, self.path contains the URL path in the request, e.g. /MyServer
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len)
        content = xmltodict.parse(post_body.decode('utf-8'))

        session_id = content['records']['@session']  # This value changes between CallBridge restarts

        if '@callBridge' in content['records']:
            callbridge_id = content['records']['@callBridge']  # Only present in clustered deployments
        else:
            callbridge_id = ""

        # Check if there is more then one record in this request
        if type(content['records']['record']) == xmltodict.OrderedDict:
            record_type = content['records']['record']['@type']
            cdr = content['records']['record']
            self.parse_cdr(record_type, cdr, session_id, callbridge_id)

        elif type(content['records']['record']) == list:
            for cdr in content['records']['record']:
                record_type = cdr['@type']
                self.parse_cdr(record_type, cdr, session_id, callbridge_id)

    def parse_cdr(self, record_type, cdr, session_id, callbridge_id):
        """
        Accepts in the CDR data and parses it to produce a JSON string
        The JSON string is written to disk for later input into MongoDB
        :param record_type:
        :param cdr:
        :param session_id:
        :param callbridge_id:
        :return:
        """

        time_stamp = cdr['@time']
        correlator_index = cdr['@correlatorIndex']

        # Progressively build the JSON depending on which values are present
        # No need to include blank data after all!

        json_output = '{{"session_id": "{session_id}"'.format(session_id=session_id)

        if len(callbridge_id) > 0:
            json_output += ', "callbridge_id": "{callbridge_id}"'.format(callbridge_id=callbridge_id)

        json_output += ', "record_type": "{record_type}"' \
                       ', "time_stamp": "{time_stamp}"' \
                       ', "correlator_index": "{correlator_index}"'.format(
                            record_type=record_type,
                            time_stamp=time_stamp,
                            correlator_index=correlator_index)

        if record_type == "callStart":
            if '@id' in cdr['call']:
                json_output += ', "call_id": "{x}"'.format(x=cdr['call']['@id'])

            if 'name' in cdr['call']:
                json_output += ', "call_name": "{x}"'.format(x=cdr['call']['name'])

            if 'callType' in cdr['call']:
                json_output += ', "call_type": "{x}"'.format(x=cdr['call']['callType'])

            if 'coSpace' in cdr['call']:
                json_output += ', "cospace": "{x}"'.format(x=cdr['call']['coSpace'])

            if 'tenant' in cdr['call']:
                json_output += ', "tenant": "{x}"'.format(x=cdr['call']['tenant'])

            if 'callCorrelator' in cdr['call']:
                json_output += ', "call_correlator": "{x}"'.format(x=cdr['call']['callCorrelator'])

            else:
                json_output += ', "db_record_type": "cdr_acano"}'

            json_output += ', "db_record_type": "cdr_acano"}'

            self.write_to_disk(json_output, log_file)

        elif record_type == "callEnd":
            if '@id' in cdr['call']:
                json_output += ', "call_id": "{x}"'.format(x=cdr['call']['@id'])

            if 'callLegsCompleted' in cdr['call']:
                json_output += ', "call_legs_completed": "{x}"'.format(x=cdr['call']['callLegsCompleted'])

            if 'callLegsMaxActive' in cdr['call']:
                json_output += ', "call_legs_max_active": "{x}"'.format(x=cdr['call']['callLegsMaxActive'])

            if 'durationSeconds' in cdr['call']:
                json_output += ', "duration_seconds": "{x}"'.format(x=cdr['call']['durationSeconds'])

            else:
                json_output += ', "db_record_type": "cdr_acano"}'

            json_output += ', "db_record_type": "cdr_acano"}'

            self.write_to_disk(json_output, log_file)

        elif record_type == "callLegStart":
            if 'call' in cdr['callLeg']:
                json_output += ', "call_id": "{x}"'.format(x=cdr['callLeg']['call'])

            if '@id' in cdr['callLeg']:
                json_output += ', "call_leg_id": "{x}"'.format(x=cdr['callLeg']['@id'])

            if 'remoteParty' in cdr['callLeg']:
                json_output += ', "remote_party": "{x}"'.format(x=cdr['callLeg']['remoteParty'])

            if 'localAddress' in cdr['callLeg']:
                json_output += ', "local_address": "{x}"'.format(x=cdr['callLeg']['localAddress'])

            if 'displayName' in cdr['callLeg']:
                json_output += ', "display_name": "{x}"'.format(x=cdr['callLeg']['displayName'])

            if 'remoteAddress' in cdr['callLeg']:
                json_output += ', "remote_address": "{x}"'.format(x=cdr['callLeg']['remoteAddress'])

            if 'type' in cdr['callLeg']:
                json_output += ', "call_leg_type": "{x}"'.format(x=cdr['callLeg']['type'])

            if 'direction' in cdr['callLeg']:
                json_output += ', "direction": "{x}"'.format(x=cdr['callLeg']['direction'])

            if 'groupId' in cdr['callLeg']:
                json_output += ', "group_id": "{x}"'.format(x=cdr['callLeg']['groupId'])

            if 'sipCallId' in cdr['callLeg']:
                json_output += ', "sip_call_id": "{x}"'.format(x=cdr['callLeg']['sipCallId'])

            else:
                json_output += ', "db_record_type": "cdr_acano"}'

            json_output += ', "db_record_type": "cdr_acano"}'

            self.write_to_disk(json_output, log_file)

        elif record_type == "callLegEnd":
            if '@id' in cdr['callLeg']:
                json_output += ', "call_leg_id": "{x}"'.format(x=cdr['callLeg']['@id'])

            if 'reason' in cdr['callLeg']:
                json_output += ', "reason": "{x}"'.format(x=cdr['callLeg']['reason'])

            if 'remoteTeardown' in cdr['callLeg']:
                json_output += ', "remote_teardown": "{x}"'.format(
                    x=cdr['callLeg']['remoteTeardown'])

            if 'durationSeconds' in cdr['callLeg']:
                json_output += ', "duration_seconds": "{x}"'.format(
                    x=cdr['callLeg']['durationSeconds'])

            if 'activeDuration' in cdr['callLeg']:
                json_output += ', "active_duration": "{x}"'.format(
                    x=cdr['callLeg']['activeDuration'])

            if 'mediaUsagePercentage' in cdr['callLeg']:

                if 'mainVideoViewer' in cdr['callLeg']['mediaUsagePercentage']:
                    json_output += ', "media_usage_main_video_viewer": "{x}"'.format(
                        x=cdr['callLeg']['mediaUsagePercentage']['mainVideoViewer'])

                if 'mainVideoContributor' in cdr['callLeg']['mediaUsagePercentage']:
                    json_output += ', "media_usage_main_video_contributor": "{x}"'.format(
                        x=cdr['callLeg']['mediaUsagePercentage']['mainVideoContributor'])

                if 'presentationViewer' in cdr['callLeg']['mediaUsagePercentage']:
                    json_output += ', "media_usage_main_presentation_viewer": "{x}"'.format(
                        x=cdr['callLeg']['mediaUsagePercentage']['presentationViewer'])

                if 'presentationContributor' in cdr['callLeg']['mediaUsagePercentage']:
                    json_output += ', "media_usage_main_presentation_contributor": "{x}"'.format(
                        x=cdr['callLeg']['mediaUsagePercentage']['presentationContributor'])

            if 'unencryptedMedia' in cdr['callLeg']:
                json_output += ', "unencrypted_media": "{x}"'.format(x=cdr['callLeg']['unencryptedMedia'])

            if 'encryptedMedia' in cdr['callLeg']:
                json_output += ', "encrypted_media": "{x}"'.format(x=cdr['callLeg']['encryptedMedia'])

            if 'rxAudio' in cdr['callLeg']:
                json_output += ', "rx_audio": {'

                if 'codec' in cdr['callLeg']['rxAudio']:
                    json_output += '"rx_audio_codec": "{x}"'.format(x=cdr['callLeg']['rxAudio']['codec'])

                if 'duration' in cdr['callLeg']['rxAudio']['packetStatistics']['packetLossBursts']:
                    json_output += ', "rx_audio_packet_loss_bursts_duration": "{x}"'.format(
                        x=cdr['callLeg']['rxAudio']['packetStatistics']['packetLossBursts']['duration'])

                if 'density' in cdr['callLeg']['rxAudio']['packetStatistics']['packetLossBursts']:
                    json_output += ', "rx_audio_packet_loss_bursts_density": "{x}"'.format(
                        x=cdr['callLeg']['rxAudio']['packetStatistics']['packetLossBursts']['density'])

                if 'duration' in cdr['callLeg']['rxAudio']['packetStatistics']['packetGap']:
                    json_output += ', "rx_audio_packet_gap_duration": "{x}"'.format(
                        x=cdr['callLeg']['rxAudio']['packetStatistics']['packetGap']['duration'])

                if 'density' in cdr['callLeg']['rxAudio']['packetStatistics']['packetGap']:
                    json_output += ', "rx_audio_packet_gap_density": "{x}"'.format(
                        x=cdr['callLeg']['rxAudio']['packetStatistics']['packetGap']['density'])

                json_output += '}'

            if 'txAudio' in cdr['callLeg']:
                json_output += ', "tx_audio": {'

                if 'codec' in cdr['callLeg']['txAudio']:
                    json_output += '"tx_audio_codec": "{x}"'.format(x=cdr['callLeg']['txAudio']['codec'])

                json_output += '}'

            if 'rxVideo' in cdr['callLeg']:
                json_output += ', "rx_video": {'

                if 'codec' in cdr['callLeg']['rxVideo']:
                    json_output += '"rx_video_codec": "{x}"'.format(x=cdr['callLeg']['rxVideo']['codec'])

                if 'packetStatistics' in cdr['callLeg']['rxVideo']:

                    if 'duration' in cdr['callLeg']['rxVideo']['packetStatistics']['packetLossBursts']:
                        json_output += ', "rx_video_packet_loss_bursts_duration": "{x}"'.format(
                            x=cdr['callLeg']['rxVideo']['packetStatistics']['packetLossBursts']['duration'])

                    if 'density' in cdr['callLeg']['rxVideo']['packetStatistics']['packetLossBursts']:
                        json_output += ', "rx_video_packet_loss_bursts_density": "{x}"'.format(
                            x=cdr['callLeg']['rxVideo']['packetStatistics']['packetLossBursts']['density'])

                    if 'duration' in cdr['callLeg']['rxVideo']['packetStatistics']['packetGap']:
                        json_output += ', "rx_video_packet_gap_duration": "{x}"'.format(
                            x=cdr['callLeg']['rxVideo']['packetStatistics']['packetGap']['duration'])

                    if 'density' in cdr['callLeg']['rxVideo']['packetStatistics']['packetGap']:
                        json_output += ', "rx_video_packet_gap_density": "{x}"'.format(
                            x=cdr['callLeg']['rxVideo']['packetStatistics']['packetGap']['density'])

                json_output += '}'

            if 'txVideo' in cdr['callLeg']:
                json_output += ', "tx_video": {'

                if 'codec' in cdr['callLeg']['txVideo']:
                    json_output += '"tx_video_codec": "{x}"'.format(x=cdr['callLeg']['txVideo']['codec'])

                json_output += '}'

            if 'sipCallId' in cdr['callLeg']:
                json_output += ', "sip_call_id": "{x}"'.format(x=cdr['callLeg']['sipCallId'])

            json_output += ', "db_record_type": "cdr_acano"}'

            self.write_to_disk(json_output, log_file)

        elif record_type == "callLegUpdate":

            if '@id' in cdr['callLeg']:
                json_output += ', "call_leg_id": "{x}"'.format(x=cdr['callLeg']['@id'])

            if 'state' in cdr['callLeg']:
                json_output += ', "state": "{x}"'.format(x=cdr['callLeg']['state'])

            if 'call' in cdr['callLeg']:
                json_output += ', "call_id": "{x}"'.format(x=cdr['callLeg']['call'])

            if 'deactivated' in cdr['callLeg']:
                json_output += ', "deactivated": "{x}"'.format(x=cdr['callLeg']['deactivated'])

            if 'groupId' in cdr['callLeg']:
                json_output += ', "group_id": "{x}"'.format(x=cdr['callLeg']['groupId'])

            if 'sipCallId' in cdr['callLeg']:
                json_output += ', "sip_call_id": "{x}"'.format(x=cdr['callLeg']['sipCallId'])

            if 'displayName' in cdr['callLeg']:
                json_output += ', "display_name": "{x}"'.format(x=cdr['callLeg']['displayName'])

            if 'remoteAddress' in cdr['callLeg']:
                json_output += ', "remote_address": "{x}"'.format(x=cdr['callLeg']['remoteAddress'])

            json_output += ', "db_record_type": "cdr_acano"}'
            self.write_to_disk(json_output, log_file)

        else:
            pass

    @staticmethod
    def write_to_disk(json_output, dest_file):
        """
        Accepts in the parsed JSON CDR data and write it to disk
        If the destination file reaches 100MB in size it will be renamed with a timestamp suffix
        and future logs will be written to a new copy of the requested destination file
        :param json_output:
        :param dest_file:
        :return:
        """

        if os.path.isfile(dest_file):
            statinfo = os.stat(dest_file)
            if statinfo.st_size > 100000000:  # 100 MB
                rotate_dest = dest_file + "." + str(datetime.datetime.utcnow().strftime('%Y_%m_%dT%H_%M_%S'))
                os.rename(dest_file, rotate_dest)

        file = open(dest_file, 'a')
        file.write(json_output)
        file.write("\n")
        file.close()


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "f:", ["--file="])
    except getopt.GetoptError:
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-f', '--file'):
            global log_file
            log_file = arg
            server_address = ('', 9999)
            httpd = HTTPServer(server_address, S)
            print("Starting httpd")
            httpd.serve_forever()

if __name__ == "__main__":
    main(sys.argv[1:])