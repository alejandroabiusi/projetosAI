import requests
from bs4 import BeautifulSoup

url = "https://www.planoeplano.com.br/portfolio/sp/sao-paulo/zona-sul/home-vila-andrade/"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
resp = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")

for img in soup.find_all("img"):
    src = img.get("src", "") or img.get("data-src", "")
    if src:
        print(src[:120])
