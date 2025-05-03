from fastapi import FASTAPI

app = FASTAPI()

@app.get("/")
def read_root():
    return{'mensagem': "API está rodando normalmente!"}