import hashlib
import json

import pulsar


def get_authentication(access_id, access_key):
    if not access_id or not access_key:
        raise ValueError("Tuya access id and access key are required for Pulsar authentication.")

    md5_access_key = hashlib.md5(access_key.encode("utf-8")).hexdigest()
    md5_combined = hashlib.md5((access_id + md5_access_key).encode("utf-8")).hexdigest()
    password = md5_combined[8:24]
    auth_params = json.dumps(
        {
            "username": access_id,
            "password": password,
            "method": "auth1",
        },
        separators=(",", ":"),
    )
    return pulsar.AuthenticationBasic(auth_params_string=auth_params)
