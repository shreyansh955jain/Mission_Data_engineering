from resources.dev import config
import base64

def validate_base64(encoded_str):
    try:
        base64.b64decode(encoded_str, validate=True)
        print("Valid Base64 string:", encoded_str)
    except Exception as e:
        print(f"Invalid Base64 encoding: {e}")

validate_base64(config.aws_access_key)
validate_base64(config.aws_secret_key)
