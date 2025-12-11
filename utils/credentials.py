from sqlalchemy.orm import Session
from model import engine, Credential
from config import settings

def get_api_credentials():
    try:
        with Session(engine) as session:
            cred = session.query(Credential).first()
            if cred:
                return int(cred.api_id), cred.api_hash
    except Exception:
        pass
    env_id = getattr(settings, 'TELEGRAM_API_ID', None)
    env_hash = getattr(settings, 'TELEGRAM_API_HASH', None)
    if env_id and env_hash:
        try:
            return int(env_id), env_hash
        except Exception:
            pass
    return None, None
