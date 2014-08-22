    
import yaml
class Constants:
    
    # TODO make this use pathlib?
    # SERVER_WORKING_DIR = "/var/www/sunspot"
    # GTFS_ZIP_FILE_STRING = os.path.join(SERVER_WORKING_DIR, "gtfs.zip")
    # GTFS_ZIP_FILE_URL = "http://suntran.com/gtfs/SuntranGTFS.zip"
    # HTTP_HEADER_LAST_MODIFIED_FORMAT = "ddd, D MMM YYYY HH:mm:ss"
    # CONFIG_FILE_PATH = "/var/www/sunspot/config.yaml"
    # ZERO_VERSION_UUID = uuid.UUID('00000200-0000-0000-0000-000000000000')
    # REMOTE_ENDPOINT = "http://mgrandi.no-ip.org:81/sunspot"
    # CHOWN_OWNER = "mark"
    # CHOWN_GROUP = "www-data"
    # DB_FOLDER = "/var/www/sunspot/dbs/"
    # CONFIG_DB_PATH = "/var/www/sunspot/configdb/"



    def __init__(self, yamlFile):
        '''constructor, takes a filepath to a yamlFile that loads the constants'''

        self.yamlConfig = yaml.safe_load(open(yamlFile, "rb"))



    def __getattr__(self, name):
        ''' implements attribute access'''


        if name in self.yamlConfig.keys():
            return self.yamlConfig[name]
        else:
            raise AttributeError("{} not found in our config".format(name))