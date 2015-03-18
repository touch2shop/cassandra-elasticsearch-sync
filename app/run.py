from app import application
from app.settings import Settings


_SETTINGS_FILE_NAME = "settings.yaml"


def main():
    settings = Settings.load_from_file(_SETTINGS_FILE_NAME)
    application.run(settings)

if __name__ == "__main__":
    main()
