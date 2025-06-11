import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class client:

    def __init__(self, token):
        try:
            from slack_sdk import WebClient
            from slack_sdk.errors import SlackApiError
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "slack-sdk"])

        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
        self.client = WebClient(token=token)

    def notify(self, channel, msg):
        try:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": msg,
                    },
                },
            ]
            response = self.client.chat_postMessage(channel=channel,
                                                    text='airflow message',
                                                    blocks=blocks)
        except SlackApiError as e:
            print(e)
