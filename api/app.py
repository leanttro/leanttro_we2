"""
ESTOQUE WPP — Backend unificado
Gerenciador de estoque, custos e vendas via WhatsApp (formulário ou IA/Groq)
"""
import os, json, csv, io, bcrypt, re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import jwt
import httpx

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
SECRET_KEY    = os.getenv("SECRET_KEY", "troca-isso-em-producao")
ADMIN_TOKEN   = os.getenv("ADMIN_TOKEN", "troca-isso-tambem")
GROQ_API_KEY_1 = os.getenv("GROQ_API_KEY_1", os.getenv("GROQ_API_KEY", ""))
GROQ_API_KEY_2 = os.getenv("GROQ_API_KEY_2", "")
BAILEYS_URL   = os.getenv("BAILEYS_URL", "http://baileys:3000")
ALGORITHM     = "HS256"
TOKEN_EXP     = 24 * 7  # horas
DB_URL        = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/postgres")
GROQ_API_URL  = "https://api.groq.com/openai/v1/chat/completions"

GROQ_MODELOS_FALLBACK = [
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
    "gemma2-9b-it",
]

MENU_TEXTO = (
    "📋 *Menu*\n"
    "1️⃣ Registrar entrada de estoque\n"
    "2️⃣ Registrar venda\n"
    "3️⃣ Consultar estoque de um produto\n"
    "4️⃣ Ajuste manual\n"
    "5️⃣ Resumo do dia\n\n"
    "Responda com o número da opção."
)

# ─────────────────────────────────────────
#  BANCO
# ─────────────────────────────────────────
def get_conn_raw():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def get_db():
    conn = get_conn_raw()
    try:
        yield conn
    finally:
        conn.close()

def db_one(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else None

def db_all(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

def db_exec(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    try:
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None

# ─────────────────────────────────────────
#  AUTH — clientes (painel)
# ─────────────────────────────────────────
security = HTTPBearer()

def criar_token(cliente_id: int, email: str) -> str:
    payload = {
        "sub": str(cliente_id),
        "email": email,
        "exp": datetime.utcnow() + timedelta(hours=TOKEN_EXP)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def get_current_cliente(
    creds: HTTPAuthorizationCredentials = Depends(security),
    conn=Depends(get_db)
):
    try:
        payload = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        cliente_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido")
    cliente = db_one(conn, "SELECT * FROM clientes WHERE id = %s AND ativo = TRUE", (cliente_id,))
    if not cliente:
        raise HTTPException(status_code=401, detail="Conta não encontrada ou inativa")
    return cliente

def check_admin(authorization: str = Header(default="")):
    """
    Aceita tanto 'Authorization: Bearer <token>' (usado pelo login.html)
    quanto o token puro no header, para compatibilidade.
    """
    token = authorization.replace("Bearer ", "").strip() if authorization else ""
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token de admin inválido")
    return True

# ─────────────────────────────────────────
#  SCHEMAS
# ─────────────────────────────────────────
class LoginBody(BaseModel):
    email: str
    senha: str

class AdminCriarClienteBody(BaseModel):
    nome_negocio: str
    email: str
    senha: str
    plano: str = "formulario"  # formulario | ia

class AdminPlanoBody(BaseModel):
    plano: str

class AdminAtivoBody(BaseModel):
    ativo: bool

class AdminNumeroBody(BaseModel):
    numero: str
    nome: Optional[str] = None

class AdminTrocaNumeroBody(BaseModel):
    numero_novo: str
    motivo: Optional[str] = None

class ProdutoBody(BaseModel):
    nome: str
    sku: Optional[str] = None
    custo_unitario: Optional[float] = 0
    preco_venda: Optional[float] = 0
    estoque_atual: Optional[float] = 0
    unidade: Optional[str] = "un"

class MovimentacaoManualBody(BaseModel):
    produto_id: int
    tipo: str  # entrada | saida | venda | ajuste
    quantidade: float
    valor_unitario: Optional[float] = 0

# ─────────────────────────────────────────
#  HELPERS DE NEGÓCIO
# ─────────────────────────────────────────
def normalizar_numero(remote_jid_ou_numero: str) -> str:
    n = remote_jid_ou_numero.split("@")[0]
    return re.sub(r"\D", "", n)

def buscar_numero_autorizado(conn, numero: str):
    return db_one(conn, "SELECT * FROM numeros_autorizados WHERE numero = %s AND ativo = TRUE", (numero,))

def get_or_create_sessao(conn, numero_autorizado_id: int):
    sessao = db_one(conn, "SELECT * FROM sessoes_conversa WHERE numero_autorizado_id = %s", (numero_autorizado_id,))
    if sessao:
        return sessao
    return db_exec(conn, """
        INSERT INTO sessoes_conversa (numero_autorizado_id, etapa_atual, dados_parciais)
        VALUES (%s, 'menu', '{}') RETURNING *
    """, (numero_autorizado_id,))

def salvar_sessao(conn, numero_autorizado_id: int, etapa: str, dados: dict):
    db_exec(conn, """
        UPDATE sessoes_conversa
        SET etapa_atual = %s, dados_parciais = %s, atualizado_em = NOW()
        WHERE numero_autorizado_id = %s
    """, (etapa, json.dumps(dados), numero_autorizado_id))

def buscar_produto_por_nome(conn, cliente_id: int, nome: str):
    return db_one(conn, """
        SELECT * FROM produtos
        WHERE cliente_id = %s AND ativo = TRUE AND nome ILIKE %s
        ORDER BY id LIMIT 1
    """, (cliente_id, f"%{nome.strip()}%"))

def aplicar_movimentacao(conn, cliente_id, produto_id, numero_autorizado_id, tipo, quantidade, valor_unitario, origem, mensagem_original=None):
    quantidade = float(quantidade)
    valor_unitario = float(valor_unitario or 0)
    valor_total = round(quantidade * valor_unitario, 2)

    db_exec(conn, """
        INSERT INTO movimentacoes
            (cliente_id, produto_id, numero_autorizado_id, tipo, quantidade, valor_unitario, valor_total, origem, mensagem_original)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (cliente_id, produto_id, numero_autorizado_id, tipo, quantidade, valor_unitario, valor_total, origem, mensagem_original))

    if tipo == "entrada":
        db_exec(conn, "UPDATE produtos SET estoque_atual = estoque_atual + %s, custo_unitario = %s WHERE id = %s",
                (quantidade, valor_unitario, produto_id))
    elif tipo in ("saida", "venda"):
        db_exec(conn, "UPDATE produtos SET estoque_atual = estoque_atual - %s WHERE id = %s", (quantidade, produto_id))
        if tipo == "venda" and valor_unitario:
            db_exec(conn, "UPDATE produtos SET preco_venda = %s WHERE id = %s", (valor_unitario, produto_id))
    elif tipo == "ajuste":
        db_exec(conn, "UPDATE produtos SET estoque_atual = %s WHERE id = %s", (quantidade, produto_id))

    return valor_total

# ─────────────────────────────────────────
#  IA (GROQ) — extração estruturada
# ─────────────────────────────────────────
def get_groq_key(cliente: dict) -> str:
    if cliente.get("groq_key_override"):
        return cliente["groq_key_override"]
    return GROQ_API_KEY_1 or GROQ_API_KEY_2

PROMPT_EXTRACAO = """Você é um extrator de dados de estoque. O usuário vai descrever uma movimentação em linguagem natural (entrada de mercadoria, venda ou saída).

Responda APENAS com um JSON válido, sem nenhum texto antes ou depois, no formato:
{"tipo": "entrada|venda|saida", "produto": "nome do produto", "quantidade": numero, "valor_unitario": numero}

Se não conseguir identificar algum campo com confiança, use null nesse campo.
Se a mensagem não for sobre estoque/venda, responda: {"tipo": null}
"""

async def chamar_groq_json(texto_usuario: str, groq_key: str) -> Optional[dict]:
    if not groq_key:
        return None
    messages = [
        {"role": "system", "content": PROMPT_EXTRACAO},
        {"role": "user", "content": texto_usuario}
    ]
    headers = {"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=25) as client:
        for modelo in GROQ_MODELOS_FALLBACK:
            payload = {"model": modelo, "temperature": 0.1, "max_tokens": 200, "messages": messages}
            try:
                resp = await client.post(GROQ_API_URL, headers=headers, json=payload)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                bruto = data["choices"][0]["message"]["content"].strip()
                bruto = re.sub(r"^```json|```$", "", bruto, flags=re.MULTILINE).strip()
                return json.loads(bruto)
            except Exception:
                continue
    return None

async def enviar_whatsapp(numero: str, texto: str):
    payload = {"number": numero, "message": texto}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            await client.post(f"{BAILEYS_URL}/disparar", json=payload)
        except Exception as e:
            print(f"⚠️ Erro ao enviar WhatsApp: {e}")

# ─────────────────────────────────────────
#  MÁQUINA DE ESTADOS — modo formulário
# ─────────────────────────────────────────
ETAPAS_TIPO = {"entrada": "entrada", "venda": "venda", "saida": "saida", "ajuste": "ajuste"}

def resposta_menu():
    return MENU_TEXTO

async def processar_texto(conn, cliente: dict, numero_autorizado: dict, texto: str) -> str:
    numero_autorizado_id = numero_autorizado["id"]
    sessao = get_or_create_sessao(conn, numero_autorizado_id)
    etapa = sessao["etapa_atual"]
    dados = sessao["dados_parciais"] if isinstance(sessao["dados_parciais"], dict) else json.loads(sessao["dados_parciais"] or "{}")
    texto_low = texto.strip().lower()

    if texto_low in ("menu", "cancelar", "0"):
        salvar_sessao(conn, numero_autorizado_id, "menu", {})
        return resposta_menu()

    # ── ETAPA: MENU ──
    if etapa == "menu":
        opcoes = {
            "1": ("entrada", "Qual produto? (nome)"),
            "2": ("venda", "Qual produto? (nome)"),
            "3": ("consulta", "Qual produto você quer consultar?"),
            "4": ("ajuste", "Qual produto? (nome)"),
            "5": ("resumo", None),
        }
        if texto.strip() in opcoes:
            tipo, pergunta = opcoes[texto.strip()]
            if tipo == "resumo":
                return await gerar_resumo_dia(conn, cliente["id"])
            salvar_sessao(conn, numero_autorizado_id, f"{tipo}_produto", {"tipo": tipo})
            return pergunta

        # modo IA: tenta extrair da mensagem livre antes de cair no menu
        if cliente["plano"] == "ia":
            extraido = await chamar_groq_json(texto, get_groq_key(cliente))
            if extraido and extraido.get("tipo") in ("entrada", "venda", "saida"):
                return await preparar_confirmacao_ia(conn, cliente, numero_autorizado_id, extraido, texto)

        return "Não entendi 🤔\n\n" + resposta_menu()

    # ── ETAPA: pedindo nome do produto ──
    if etapa.endswith("_produto"):
        tipo = dados.get("tipo")
        if tipo == "consulta":
            produto = buscar_produto_por_nome(conn, cliente["id"], texto)
            salvar_sessao(conn, numero_autorizado_id, "menu", {})
            if not produto:
                return f"Produto '{texto}' não encontrado.\n\n" + resposta_menu()
            return (
                f"📦 *{produto['nome']}*\n"
                f"Estoque atual: {produto['estoque_atual']} {produto['unidade']}\n"
                f"Custo: R$ {produto['custo_unitario']:.2f}\n"
                f"Preço de venda: R$ {produto['preco_venda']:.2f}\n\n"
            ) + resposta_menu()

        produto = buscar_produto_por_nome(conn, cliente["id"], texto)
        if not produto:
            produto = db_exec(conn, """
                INSERT INTO produtos (cliente_id, nome) VALUES (%s, %s) RETURNING *
            """, (cliente["id"], texto.strip()))
        dados["produto_id"] = produto["id"]
        dados["produto_nome"] = produto["nome"]
        salvar_sessao(conn, numero_autorizado_id, f"{tipo}_quantidade", dados)
        return f"Quantidade de *{produto['nome']}*?"

    # ── ETAPA: pedindo quantidade ──
    if etapa.endswith("_quantidade"):
        tipo = dados.get("tipo")
        try:
            quantidade = float(texto.replace(",", "."))
        except ValueError:
            return "Manda só o número da quantidade, por favor."
        dados["quantidade"] = quantidade
        if tipo == "ajuste":
            salvar_sessao(conn, numero_autorizado_id, "confirmando", dados)
            return (
                f"Confirma o ajuste de estoque de *{dados['produto_nome']}* para "
                f"{quantidade}? Responda SIM ou NÃO."
            )
        salvar_sessao(conn, numero_autorizado_id, f"{tipo}_valor", dados)
        rotulo = "custo unitário (R$)" if tipo == "entrada" else "valor unitário de venda (R$)"
        return f"Qual o {rotulo}?"

    # ── ETAPA: pedindo valor ──
    if etapa.endswith("_valor"):
        tipo = dados.get("tipo")
        try:
            valor = float(texto.replace(",", "."))
        except ValueError:
            return "Manda só o valor em número, por favor."
        dados["valor_unitario"] = valor
        total = round(valor * dados["quantidade"], 2)
        dados["valor_total"] = total
        salvar_sessao(conn, numero_autorizado_id, "confirmando", dados)
        acao = {"entrada": "Entrada de", "venda": "Venda de", "saida": "Saída de"}[tipo]
        return (
            f"Confirma?\n"
            f"{acao} {dados['quantidade']} × *{dados['produto_nome']}* a R$ {valor:.2f} "
            f"(total R$ {total:.2f})\n\nResponda SIM ou NÃO."
        )

    # ── ETAPA: confirmando ──
    if etapa == "confirmando":
        if texto_low in ("sim", "s", "confirmo", "confirmar"):
            tipo = dados.get("tipo")
            aplicar_movimentacao(
                conn, cliente["id"], dados["produto_id"], numero_autorizado_id,
                tipo, dados["quantidade"], dados.get("valor_unitario", 0),
                origem="formulario", mensagem_original=texto
            )
            salvar_sessao(conn, numero_autorizado_id, "menu", {})
            return "✅ Registrado com sucesso!\n\n" + resposta_menu()
        if texto_low in ("não", "nao", "n"):
            salvar_sessao(conn, numero_autorizado_id, "menu", {})
            return "Cancelado.\n\n" + resposta_menu()
        return "Responda SIM ou NÃO."

    # fallback de segurança
    salvar_sessao(conn, numero_autorizado_id, "menu", {})
    return resposta_menu()

async def preparar_confirmacao_ia(conn, cliente, numero_autorizado_id, extraido, texto_original):
    tipo = extraido.get("tipo")
    nome_produto = extraido.get("produto")
    quantidade = extraido.get("quantidade")
    valor_unitario = extraido.get("valor_unitario") or 0

    if not nome_produto or quantidade is None:
        return "Entendi que é sobre estoque, mas faltou produto ou quantidade. Pode reescrever?\n\n" + resposta_menu()

    produto = buscar_produto_por_nome(conn, cliente["id"], nome_produto)
    if not produto:
        produto = db_exec(conn, "INSERT INTO produtos (cliente_id, nome) VALUES (%s,%s) RETURNING *",
                           (cliente["id"], nome_produto.strip()))

    dados = {
        "tipo": tipo, "produto_id": produto["id"], "produto_nome": produto["nome"],
        "quantidade": float(quantidade), "valor_unitario": float(valor_unitario or 0),
    }
    salvar_sessao(conn, numero_autorizado_id, "confirmando", dados)
    total = round(dados["quantidade"] * dados["valor_unitario"], 2)
    acao = {"entrada": "Entrada de", "venda": "Venda de", "saida": "Saída de"}[tipo]
    return (
        f"Confirma?\n{acao} {dados['quantidade']} × *{produto['nome']}* "
        f"a R$ {dados['valor_unitario']:.2f} (total R$ {total:.2f})\n\nResponda SIM ou NÃO."
    )

async def gerar_resumo_dia(conn, cliente_id: int) -> str:
    hoje = datetime.utcnow().date()
    linhas = db_all(conn, """
        SELECT tipo, COALESCE(SUM(quantidade),0) qtd, COALESCE(SUM(valor_total),0) total
        FROM movimentacoes
        WHERE cliente_id = %s AND criado_em::date = %s
        GROUP BY tipo
    """, (cliente_id, hoje))
    if not linhas:
        return f"📊 Nenhuma movimentação hoje ({hoje.strftime('%d/%m')}).\n\n" + resposta_menu()
    txt = f"📊 *Resumo de hoje ({hoje.strftime('%d/%m')})*\n"
    for l in linhas:
        txt += f"- {l['tipo'].capitalize()}: {l['qtd']} un | R$ {l['total']:.2f}\n"
    return txt + "\n" + resposta_menu()

# ─────────────────────────────────────────
#  LIFESPAN
# ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = get_conn_raw()
        conn.close()
        print("✅ Conexão com banco OK")
    except Exception as e:
        print(f"⚠️ Não foi possível conectar ao banco no startup: {e}")
    yield

app = FastAPI(title="Estoque WPP", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ═════════════════════════════════════════
#  AUTH — CLIENTE
# ═════════════════════════════════════════
@app.post("/auth/login")
def login(body: LoginBody, conn=Depends(get_db)):
    cliente = db_one(conn, "SELECT * FROM clientes WHERE email = %s AND ativo = TRUE", (body.email,))
    if not cliente or not bcrypt.checkpw(body.senha.encode(), cliente["senha_hash"].encode()):
        raise HTTPException(status_code=401, detail="Email ou senha inválidos")
    token = criar_token(cliente["id"], cliente["email"])
    return {
        "access_token": token,
        "cliente_id": cliente["id"],
        "cliente": {k: v for k, v in cliente.items() if k != "senha_hash"},
    }

@app.get("/auth/me")
def me(cliente=Depends(get_current_cliente)):
    return {k: v for k, v in cliente.items() if k != "senha_hash"}

# ═════════════════════════════════════════
#  ADMIN — gestão de clientes/números/conexão
# ═════════════════════════════════════════
@app.get("/admin/clientes")
def admin_listar_clientes(conn=Depends(get_db), _admin=Depends(check_admin)):
    clientes = db_all(conn, "SELECT id, nome_negocio, email, plano, ativo, criado_em FROM clientes ORDER BY id DESC")
    for c in clientes:
        c["numeros"] = db_all(conn, "SELECT * FROM numeros_autorizados WHERE cliente_id = %s", (c["id"],))
    return clientes

@app.post("/admin/clientes")
def admin_criar_cliente(body: AdminCriarClienteBody, conn=Depends(get_db), _admin=Depends(check_admin)):
    if db_one(conn, "SELECT id FROM clientes WHERE email = %s", (body.email,)):
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    senha_hash = bcrypt.hashpw(body.senha.encode(), bcrypt.gensalt()).decode()
    cliente = db_exec(conn, """
        INSERT INTO clientes (nome_negocio, email, senha_hash, plano)
        VALUES (%s,%s,%s,%s) RETURNING id, nome_negocio, email, plano, ativo
    """, (body.nome_negocio, body.email, senha_hash, body.plano))
    return cliente

@app.patch("/admin/clientes/{cliente_id}/plano")
def admin_trocar_plano(cliente_id: int, body: AdminPlanoBody, conn=Depends(get_db), _admin=Depends(check_admin)):
    db_exec(conn, "UPDATE clientes SET plano = %s WHERE id = %s", (body.plano, cliente_id))
    return {"ok": True}

@app.patch("/admin/clientes/{cliente_id}/ativo")
def admin_toggle_ativo(cliente_id: int, body: AdminAtivoBody, conn=Depends(get_db), _admin=Depends(check_admin)):
    db_exec(conn, "UPDATE clientes SET ativo = %s WHERE id = %s", (body.ativo, cliente_id))
    return {"ok": True}

@app.delete("/admin/clientes/{cliente_id}")
def admin_deletar_cliente(cliente_id: int, conn=Depends(get_db), _admin=Depends(check_admin)):
    db_exec(conn, "DELETE FROM clientes WHERE id = %s", (cliente_id,))
    return {"ok": True}

@app.post("/admin/clientes/{cliente_id}/numeros")
def admin_add_numero(cliente_id: int, body: AdminNumeroBody, conn=Depends(get_db), _admin=Depends(check_admin)):
    numero = normalizar_numero(body.numero)
    if db_one(conn, "SELECT id FROM numeros_autorizados WHERE numero = %s", (numero,)):
        raise HTTPException(status_code=400, detail="Número já cadastrado")
    reg = db_exec(conn, """
        INSERT INTO numeros_autorizados (cliente_id, numero, nome) VALUES (%s,%s,%s) RETURNING *
    """, (cliente_id, numero, body.nome))
    return reg

@app.delete("/admin/numeros/{numero_id}")
def admin_del_numero(numero_id: int, conn=Depends(get_db), _admin=Depends(check_admin)):
    db_exec(conn, "DELETE FROM numeros_autorizados WHERE id = %s", (numero_id,))
    return {"ok": True}

@app.get("/admin/conexao")
def admin_get_conexao(conn=Depends(get_db), _admin=Depends(check_admin)):
    conexao = db_one(conn, "SELECT * FROM conexao_bot WHERE id = 1")
    try:
        async def _status():
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{BAILEYS_URL}/status")
                return r.json()
    except Exception:
        pass
    return conexao

@app.post("/admin/conexao/resetar")
async def admin_resetar_conexao(conn=Depends(get_db), _admin=Depends(check_admin)):
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            await client.post(f"{BAILEYS_URL}/resetar")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Falha ao resetar sessão no Baileys: {e}")
    db_exec(conn, "UPDATE conexao_bot SET status = 'trocando', atualizado_em = NOW() WHERE id = 1")
    return {"ok": True, "mensagem": "Sessão resetada. Acesse /whatsapp/qrcode para escanear o novo QR."}

@app.post("/admin/conexao/notificar-clientes")
async def admin_notificar_clientes(conn=Depends(get_db), _admin=Depends(check_admin)):
    conexao = db_one(conn, "SELECT * FROM conexao_bot WHERE id = 1")
    numeros = db_all(conn, "SELECT numero FROM numeros_autorizados WHERE ativo = TRUE")
    texto = (
        "📢 Aviso: este é o número oficial atualizado do sistema de estoque. "
        "Salve este contato para continuar registrando suas movimentações por aqui."
    )
    enviados = 0
    for n in numeros:
        await enviar_whatsapp(n["numero"], texto)
        enviados += 1
    return {"ok": True, "notificados": enviados}

@app.get("/admin/log-trocas")
def admin_log_trocas(conn=Depends(get_db), _admin=Depends(check_admin)):
    return db_all(conn, "SELECT * FROM log_troca_numero ORDER BY criado_em DESC")

# Chamado internamente pelo Baileys quando conecta com um número (ver webhook abaixo)
class ConexaoAtualizaBody(BaseModel):
    numero: str

@app.post("/admin/conexao/atualizar")
def conexao_atualizar(body: ConexaoAtualizaBody, conn=Depends(get_db)):
    numero = normalizar_numero(body.numero)
    atual = db_one(conn, "SELECT * FROM conexao_bot WHERE id = 1")
    if atual and atual.get("numero_atual") and atual["numero_atual"] != numero:
        db_exec(conn, "INSERT INTO log_troca_numero (numero_antigo, numero_novo) VALUES (%s,%s)",
                (atual["numero_atual"], numero))
    db_exec(conn, "UPDATE conexao_bot SET numero_atual = %s, status = 'conectado', atualizado_em = NOW() WHERE id = 1",
            (numero,))
    return {"ok": True}

# ═════════════════════════════════════════
#  PRODUTOS (painel do cliente)
# ═════════════════════════════════════════
@app.get("/produtos")
def listar_produtos(cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    return db_all(conn, "SELECT * FROM produtos WHERE cliente_id = %s AND ativo = TRUE ORDER BY nome",
                  (cliente["id"],))

@app.post("/produtos")
def criar_produto(body: ProdutoBody, cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    return db_exec(conn, """
        INSERT INTO produtos (cliente_id, nome, sku, custo_unitario, preco_venda, estoque_atual, unidade)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *
    """, (cliente["id"], body.nome, body.sku, body.custo_unitario, body.preco_venda, body.estoque_atual, body.unidade))

@app.put("/produtos/{produto_id}")
def atualizar_produto(produto_id: int, body: ProdutoBody, cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    db_exec(conn, """
        UPDATE produtos SET nome=%s, sku=%s, custo_unitario=%s, preco_venda=%s, estoque_atual=%s, unidade=%s
        WHERE id=%s AND cliente_id=%s
    """, (body.nome, body.sku, body.custo_unitario, body.preco_venda, body.estoque_atual, body.unidade,
          produto_id, cliente["id"]))
    return {"ok": True}

@app.delete("/produtos/{produto_id}")
def deletar_produto(produto_id: int, cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    db_exec(conn, "UPDATE produtos SET ativo = FALSE WHERE id = %s AND cliente_id = %s", (produto_id, cliente["id"]))
    return {"ok": True}

# ═════════════════════════════════════════
#  MOVIMENTAÇÕES / DASHBOARD
# ═════════════════════════════════════════
@app.get("/movimentacoes")
def listar_movimentacoes(
    produto_id: Optional[int] = None, tipo: Optional[str] = None,
    de: Optional[str] = None, ate: Optional[str] = None,
    cliente=Depends(get_current_cliente), conn=Depends(get_db)
):
    sql = """
        SELECT m.*, p.nome AS produto_nome, n.nome AS registrado_por
        FROM movimentacoes m
        JOIN produtos p ON p.id = m.produto_id
        LEFT JOIN numeros_autorizados n ON n.id = m.numero_autorizado_id
        WHERE m.cliente_id = %s
    """
    params = [cliente["id"]]
    if produto_id:
        sql += " AND m.produto_id = %s"; params.append(produto_id)
    if tipo:
        sql += " AND m.tipo = %s"; params.append(tipo)
    if de:
        sql += " AND m.criado_em::date >= %s"; params.append(de)
    if ate:
        sql += " AND m.criado_em::date <= %s"; params.append(ate)
    sql += " ORDER BY m.criado_em DESC LIMIT 500"
    return db_all(conn, sql, tuple(params))

@app.post("/movimentacoes")
def criar_movimentacao_manual(body: MovimentacaoManualBody, cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    produto = db_one(conn, "SELECT * FROM produtos WHERE id = %s AND cliente_id = %s", (body.produto_id, cliente["id"]))
    if not produto:
        raise HTTPException(status_code=404, detail="Produto não encontrado")
    aplicar_movimentacao(conn, cliente["id"], body.produto_id, None, body.tipo, body.quantidade,
                          body.valor_unitario, origem="manual_admin")
    return {"ok": True}

@app.get("/dashboard/resumo")
def dashboard_resumo(cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    produtos = db_all(conn, "SELECT * FROM produtos WHERE cliente_id = %s AND ativo = TRUE", (cliente["id"],))
    valor_em_estoque = sum(float(p["estoque_atual"]) * float(p["custo_unitario"]) for p in produtos)

    hoje = datetime.utcnow().date()
    inicio_mes = hoje.replace(day=1)
    vendas_mes = db_one(conn, """
        SELECT COALESCE(SUM(valor_total),0) total, COALESCE(SUM(quantidade),0) qtd
        FROM movimentacoes WHERE cliente_id = %s AND tipo = 'venda' AND criado_em::date >= %s
    """, (cliente["id"], inicio_mes))
    custo_vendido_mes = db_all(conn, """
        SELECT m.produto_id, m.quantidade, p.custo_unitario
        FROM movimentacoes m JOIN produtos p ON p.id = m.produto_id
        WHERE m.cliente_id = %s AND m.tipo = 'venda' AND m.criado_em::date >= %s
    """, (cliente["id"], inicio_mes))
    custo_total_mes = sum(float(r["quantidade"]) * float(r["custo_unitario"]) for r in custo_vendido_mes)
    margem_mes = float(vendas_mes["total"]) - custo_total_mes

    return {
        "total_produtos": len(produtos),
        "valor_em_estoque": round(valor_em_estoque, 2),
        "produtos_estoque_baixo": [p for p in produtos if float(p["estoque_atual"]) <= 0],
        "vendas_mes_total": round(float(vendas_mes["total"]), 2),
        "vendas_mes_qtd": float(vendas_mes["qtd"]),
        "margem_mes": round(margem_mes, 2),
    }

@app.get("/dashboard/export.csv")
def export_csv(de: Optional[str] = None, ate: Optional[str] = None,
                cliente=Depends(get_current_cliente), conn=Depends(get_db)):
    sql = """
        SELECT m.criado_em, p.nome AS produto, m.tipo, m.quantidade, m.valor_unitario, m.valor_total, m.origem
        FROM movimentacoes m JOIN produtos p ON p.id = m.produto_id
        WHERE m.cliente_id = %s
    """
    params = [cliente["id"]]
    if de:
        sql += " AND m.criado_em::date >= %s"; params.append(de)
    if ate:
        sql += " AND m.criado_em::date <= %s"; params.append(ate)
    sql += " ORDER BY m.criado_em DESC"
    linhas = db_all(conn, sql, tuple(params))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Data", "Produto", "Tipo", "Quantidade", "Valor Unitário", "Valor Total", "Origem"])
    for l in linhas:
        writer.writerow([l["criado_em"], l["produto"], l["tipo"], l["quantidade"],
                          l["valor_unitario"], l["valor_total"], l["origem"]])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=movimentacoes.csv"}
    )

# ═════════════════════════════════════════
#  WHATSAPP — webhook e status
# ═════════════════════════════════════════
@app.post("/webhook/mensagem")
async def webhook_mensagem(payload: dict, conn=Depends(get_db)):
    remote_jid = payload.get("remoteJid", "")
    texto = payload.get("text")
    from_me = payload.get("fromMe", False)
    if from_me or not texto or remote_jid.endswith("@g.us"):
        return {"ok": True}

    numero = normalizar_numero(remote_jid)
    numero_autorizado = buscar_numero_autorizado(conn, numero)
    if not numero_autorizado:
        await enviar_whatsapp(numero, "❌ Número não autorizado. Fale com o administrador do sistema.")
        return {"ok": True}

    cliente = db_one(conn, "SELECT * FROM clientes WHERE id = %s AND ativo = TRUE", (numero_autorizado["cliente_id"],))
    if not cliente:
        await enviar_whatsapp(numero, "❌ Conta inativa. Fale com o administrador.")
        return {"ok": True}

    resposta = await processar_texto(conn, cliente, numero_autorizado, texto)
    await enviar_whatsapp(numero, resposta)
    return {"ok": True}

@app.get("/whatsapp/status")
async def whatsapp_status():
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BAILEYS_URL}/status")
            return r.json()
        except Exception:
            return {"connected": False}

@app.get("/whatsapp/qrcode")
async def whatsapp_qrcode():
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BAILEYS_URL}/qrcode-raw")
            return r.json()
        except Exception:
            return {"qr": None}

@app.get("/")
def root():
    return {"status": "ok", "servico": "Estoque WPP API"}
