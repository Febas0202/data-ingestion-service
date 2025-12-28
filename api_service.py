import logging
from typing import Any, Dict

import requests


def api_login(
    base_url: str,
    usuario: str,
    senha: str,
    identificador: str,
    verify_ssl: bool,
    logger: logging.Logger
) -> Dict[str, Any]:
    url = f"{base_url}/Login"
    files = {
        "usuario": (None, usuario),
        "senha": (None, senha),
        "identificador": (None, identificador),
    }

    logger.info(f"Login: POST {url} (identificador={identificador})")
    resp = requests.post(url, files=files, verify=verify_ssl, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    if not data.get("resultado", False):
        raise RuntimeError(f"Login falhou: {data}")

    sessao = data.get("sessao")
    id_usuario = data.get("id_usuario")

    if not sessao or not id_usuario:
        raise RuntimeError(f"Resposta de login sem sessao/id_usuario: {data}")

    logger.info(f"Login OK | id_usuario={id_usuario} | sessao={str(sessao)[:6]}...")

    return {
        "sessao": str(sessao),
        "idUsuario": str(id_usuario),
        "identificador": str(identificador),
        "raw": data
    }


def api_fetch_endpoint(
    base_url: str,
    endpoint: str,
    sessao: str,
    idUsuario: str,
    identificador: str,
    verify_ssl: bool,
    logger: logging.Logger
) -> Any:
    url = f"{base_url}/{endpoint}"
    files = {
        "sessao": (None, sessao),
        "idUsuario": (None, idUsuario),
        "identificador": (None, identificador),
    }

    logger.info(f"Fetch: POST {url}")
    resp = requests.post(url, files=files, verify=verify_ssl, timeout=120)
    resp.raise_for_status()

    try:
        return resp.json()
    except Exception:
        text = resp.text
        raise RuntimeError(
            f"Endpoint {endpoint} retornou não-JSON. Conteúdo inicial: {text[:300]}"
        )
    
    
# nesse arquivo api_service.py você tem funções para fazer login na API e buscar dados de endpoints específicos. Essas funções lidam com requisições HTTP, Tratamento de erros e logging das operações realizadas.      