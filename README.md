## Coinwatch

Nástroj pre automatizovanú analýzu cien kryptomenových derivátov na burze ByBit.

### Ako to funguje

Coinwatch vyhodnocuje v reálnom čase rôzne indikátory cenového pohybu jednotlivých kryptomenových derivátov na burze. Umožňuje filtrovať a zoraďovať kryptomeny na Bybite na základe vybraných indikátorov a kritérii.
Cieľ je vedieť vyhodnotiť, ktoré kryptomeny sa oplatí sledovať a obchodovať na základe zvolených kritérii (24h cenový pohyb v percentách, RSI, MACD, Bollinger Bands, Volume na 1h, 2h, 4h, dennom grafe a pod.) pretože manuálne by zabralo prechádzať vyše 500 kryptomien na Bybite veľmi dlho. 

### TODO

1. Treba naprogramovať stiahnutie cenových dát do vlastnej databázy. Malo by sa jednať o 1h dáta, ktoré potom môžme pomocou knižnice Pandas agregovať alebo grupovať do 2h, 4h, 6h, 12h alebo denných cien, a na nich potom robiť dátovú/technickú analýzu.
2. Nástroj by mal vedieť nielen stiahnuť dáta, ale aj overiť ich integritu od posledného spustenia, t.j. zistiť, či je databáza kompletná, a vložiť chýbajúce cenové údaje, aby boli aktuálne.
3. Jediné údaje, ktoré budú uložené v databáze sú 1h OHLC dáta vrátane Volume. V knižnici **Pybit** na to slúži metóda _get_kline()_. Tým pádom nebude databáza príliš obrovská.
4. Keď bude fungovať synchronizácia databázy, treba navrhnuť API, pomocou ktorého z nej bude možné vytiahnuť požadované indikátory. Užívateľ si bude môcť na frontende zvoliť, ktoré indikátory ho zaujímajú a vybrať si zobrazenie napr. v nejakej tabuľke. Na samotnom UI, a spôsobe používania aplikácie ešte popracujem.

### Usage

Run synchronization
```
docker compose up -d
```

Stop synchronization
```
docker compose stop
```

See app container logs to ensure synchronization is running 
```
docker compose logs app
```

phpMyAdmin is available on http://localhost:8080/