from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer
from kafka.errors import KafkaError

import datetime
import json


BROKERS = 'boot-3blky9rn.c3.kafka-serverless.us-east-1.amazonaws.com:9098'
# BROKERS = 'b-1.esgf2democluster.5clnrg.c21.kafka.us-east-1.amazonaws.com:9098'
REGION = 'us-east-1'


class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


def stdout(metadata):

    tp = MSKTokenProvider()
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        api_version=(3, 5, 1),
        max_block_ms=1200000,
        retry_backoff_ms=500,
        request_timeout_ms=20000,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        value_serializer=lambda m: json.dumps(
            m, indent=4, sort_keys=True, default=str).encode('utf-8'))

    message = {
        "authorization_server": "Globus",
        "created": datetime.datetime.now(),
        "event": "publish",
        "user": "lukasz@uchicago.edu",
        "metadata": metadata,

    }

    # print(message)

    future = producer.send('ESGF2DemoPublishTopic', value=message)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        print(e)

    print(record_metadata)
    producer.flush()
