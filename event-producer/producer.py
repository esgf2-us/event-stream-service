import datetime


def stdout(metadata):
    message = {
        "authorization_server": "Globus",
        "created": datetime.datetime.now(),
        "event": "publish",
        "user": "lukasz@uchicago.edu",
        "metadata": metadata,

    }
    print(message)
