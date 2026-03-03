import requests
from bs4 import BeautifulSoup

r = requests.get('https://dbie.rbi.org.in', timeout=10)
soup = BeautifulSoup(r.text, 'lxml')

keywords = ['payment', 'card', 'psi', 'excel', 'xlsx', 'csv', 'download']
links = soup.find_all('a', href=True)

found = 0
for link in links:
    href = link['href'].lower()
    text = link.get_text(strip=True).lower()
    if any(k in href or k in text for k in keywords):
        print(f"TEXT: {link.get_text(strip=True)[:60]}")
        print(f"HREF: {link['href']}")
        print("---")
        found += 1

if found == 0:
    print("No matching links found — portal may load links via JavaScript")
    print("\nAll links on page:")
    for link in links[:30]:
        print(link['href'])
