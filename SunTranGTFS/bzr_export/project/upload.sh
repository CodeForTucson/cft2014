cd ~/Code/bzr_new/winphone_tucson_suntran_app_repo/trunk
echo "Deleting"
rm project/protoc/gtfs_data_pb2.py
echo "Generating protobuf class"
protoc-py3 -I=project/protoc project/protoc/gtfs-data.proto --python_out=project/protoc
echo "Rsync-ing"
rsync3 -q --rsh='ssh -p 23' project/protoc/gtfs_data_pb2.py mark@mgrandi.no-ip.org:/var/www/sunspot
rsync3 -q --rsh='ssh -p 23' project/sunspot_server/sunspot_server.py mark@mgrandi.no-ip.org:/var/www/sunspot