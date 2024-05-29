def stdout(metadata):
    message = {
        "event": "publish",
        "user": "lukasz@uchicago.edu",
        "metadata": metadata,
    }
    print(message)
