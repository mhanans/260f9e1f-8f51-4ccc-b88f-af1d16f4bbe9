
from datetime import datetime, timedelta
from typing import Any, Union
from jose import jwt
from passlib.context import CryptContext
from app.core.config import settings

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

def create_access_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str:
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:

from cryptography.fernet import Fernet
import base64

class EncryptionUtility:
    """Handles encryption of sensitive config values (like DB connection strings)."""
    
    def __init__(self):
        # Ensure key is 32 url-safe base64-encoded bytes
        # We use SECRET_KEY. Note: In prod, use a dedicated key.
        key = settings.SECRET_KEY[:32].encode() 
        if len(key) < 32:
            key = key + b'=' * (32 - len(key))
        self.fernet = Fernet(base64.urlsafe_b64encode(key))

    def encrypt(self, plain_text: str) -> str:
        if not plain_text: return ""
        return self.fernet.encrypt(plain_text.encode()).decode()

    def decrypt(self, cipher_text: str) -> str:
        if not cipher_text: return ""
        try:
            return self.fernet.decrypt(cipher_text.encode()).decode()
        except:
            return cipher_text # Return as-is if not encrypted or error

encryption_utility = EncryptionUtility()
