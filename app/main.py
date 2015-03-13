from app.settings import Settings


_SETTINGS_FILE_NAME = "settings.yaml"


def run():
    settings = Settings.load_from_file(_SETTINGS_FILE_NAME)

if __name__ == "__main__":
    run()
