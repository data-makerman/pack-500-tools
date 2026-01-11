import requests
from bs4 import BeautifulSoup

def dump_adventure_structure(rank_slug: str = "lion") -> None:
    url = f"https://www.scouting.org/programs/cub-scouts/adventures/{rank_slug}/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.select("article.cs-adv-activity")
    print(f"found {len(articles)} adventure cards")
    for article in articles[:3]:
        classes = article.get("class", [])
        heading = article.select_one("h2 a")
        print(" -", heading.get_text(strip=True), heading.get("href"), classes)


def dump_example_adventure(slug: str = "fun-on-the-run") -> None:
    url = f"https://www.scouting.org/cub-scout-adventures/{slug}/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    heading = soup.find(lambda tag: tag.name in {"h1", "h2", "h3"} and "Complete the following requirements" in tag.get_text())
    print("requirements heading:", heading)
    if heading:
        blocks = []
        for h3 in heading.find_all_next("h3"):
            text = h3.get_text(strip=True)
            if not text.lower().startswith("requirement"):
                if "Requirement" not in text:
                    continue
            para = []
            for sib in h3.find_all_next():
                if sib == h3:
                    continue
                if sib.name == "h3" and "Requirement" in sib.get_text(strip=True):
                    break
                if sib.name == "h2":
                    break
                if sib.name == "p":
                    para.append(sib.get_text(strip=True))
            blocks.append((text, para[:3]))
            if len(blocks) == 3:
                break
        print(blocks)


if __name__ == "__main__":
    dump_adventure_structure()
    dump_example_adventure()
