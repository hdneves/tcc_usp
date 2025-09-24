import os
import requests
import pandas as pd
from dotenv import load_dotenv
from tcc.config.constants import BASE_URL

#python -m tcc.api_extractor.extractor

class TMDBClient:
    #BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str, language: str = "en-US", region: str = "US"):
        self.api_key = api_key
        self.language = language
        self.region = region

    def _get(self, endpoint: str, params: dict = None):
        if params is None:
            params = {}
        params["api_key"] = self.api_key
        params["language"] = self.language
        response = requests.get(f"{BASE_URL}{endpoint}", params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro [{response.status_code}] ao acessar {endpoint}")
            return {}

    def obter_generos(self) -> dict:
        data = self._get("/genre/movie/list")
        return {g["id"]: g["name"] for g in data.get("genres", [])}

    def buscar_em_cartaz(self, pagina: int = 1) -> tuple[list, int]:
        params = {"region": self.region, "page": pagina}
        data = self._get("/movie/now_playing", params=params)
        return data.get("results", []), data.get("total_pages", 1)

    def listar_filmes_em_cartaz(self) -> list[dict]:
        generos_dict = self.obter_generos()
        pagina = 1
        todos_filmes = []

        while True:
            filmes, total_paginas = self.buscar_em_cartaz(pagina)
            if not filmes:
                break

            for filme in filmes:
                genero_nomes = [generos_dict.get(gid, "Desconhecido") for gid in filme.get("genre_ids", [])]
                todos_filmes.append({
                    "titulo": filme["title"],
                    "data": filme.get("release_date", "sem data"),
                    "generos": genero_nomes
                })

            if pagina >= total_paginas:
                break
            pagina += 1

        return todos_filmes

    def filmes_dataframe(self) -> pd.DataFrame:
        filmes = self.listar_filmes_em_cartaz()
        df = pd.DataFrame(filmes)

        if not df.empty:
            df["ano"] = pd.to_datetime(df["data"], errors="coerce").dt.year
            df["mes"] = pd.to_datetime(df["data"], errors="coerce").dt.month
            df["generos_str"] = df["generos"].apply(lambda x: ", ".join(x))
            df = df.dropna(subset=["generos_str"])
            df = df[df["generos_str"].str.strip() != ""]
            df = df[df["data"] > "2025-01-01"]

        return df


# ===== Exemplo de uso =====
if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("API_KEY")

    client = TMDBClient(api_key=API_KEY, language="en-US", region="BR")
    df_filmes = client.filmes_dataframe()

    print(df_filmes.head())
