cd ~/Code/bzr_new/winphone_tucson_suntran_app_repo/trunk
echo "Deleting"
rm project/sunspot_server/sunspot_messages_pb2.py
echo "Generating protobuf class"
protoc-py3 -I=project/protoc project/protoc/sunspot_messages.proto --python_out=project/sunspot_server/
echo "Rsync-ing"
rsync3 -q --rsh='ssh -p 23' project/protoc/sunspot_messages_pb2.py mark@mgrandi.no-ip.org:/var/www/sunspot
rsync3 -q --rsh='ssh -p 23' project/sunspot_server/sunspot_server.py mark@mgrandi.no-ip.org:/var/www/sunspot
rsync3 -q --rsh='ssh -p 23' project/sunspot_server/constants.py mark@mgrandi.no-ip.org:/var/www/sunspot
rsync3 -q --rsh='ssh -p 23' project/sunspot_server/parse_gtfs_data.py mark@mgrandi.no-ip.org:/var/www/sunspot