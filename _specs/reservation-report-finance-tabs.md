# Spec for reservation-report-finance-tabs

branch: reservation-report-DRO-383

## Summary

Raport dzienny rezerwacji ([reservations_daily_report.py](navigation_pages/reports/reservations_daily_report.py)) generuje obecnie jeden plik `.xlsx` z arkuszami wg źródła rezerwacji (Sumarycznie / Online / Host / CC). W każdym arkuszu miasta są ułożone jedno pod drugim jako osobne tabele z scalonym nagłówkiem z nazwą miasta, a kolumny obejmują zarówno metryki finansowe, jak i porównania r/r.

Celem tej zmiany jest rozdzielenie raportu na dwie części — **Finanse** i **Marketing** — aby nie mieszać perspektyw w jednym widoku ("nie zaśmiecać raportu"):

- **Marketing** — pozostaje bez zmian (dotychczasowy raport w obecnej postaci).
- **Finanse** — nowy układ: miasta stają się kolumną (wymiarem) zamiast nagłówków osobnych tabel, dochodzą kolumny matogodzin i liczby mat.

## Functional Requirements

### Podział na dwa taby

- Strona raportu prezentuje dwa taby: **Finanse** i **Marketing**, zrealizowane przez `stx.tab_bar` (`extra_streamlit_components`) — analogicznie do [income.py:153](navigation_pages/income.py#L153); każdy tab renderuje własny widok.
- Każdy tab ma własny przycisk generowania i własny przycisk pobierania pliku `.xlsx`.
- Tab **Marketing** generuje dotychczasowy raport bez żadnych zmian w układzie, kolumnach ani arkuszach.

### Nowy układ tabu Finanse

- Miasto przestaje być scalonym nagłówkiem osobnej tabeli i staje się **kolumną** we wspólnej, płaskiej tabeli.
- Jeden wiersz odpowiada kombinacji (Data × Miasto × [Rodzaj atrakcji, gdy włączony podział]).
- Przy włączonym podziale na atrakcje dla jednego dnia (np. 01.07) powstaje do 45 wierszy (9 miast × 5 grup atrakcji).

### Nowe / zmienione kolumny w tabie Finanse

Kolejność kolumn (przy włączonym podziale na atrakcje):

1. Data
2. Miasto (**NOWA** — miasto jako kolumna)
3. Rodzaj atrakcji (gdy włączony podział)
4. Liczba rezerwacji
5. Liczba matogodzin (**NOWA** — bezpośrednio po „Liczba rezerwacji")
6. Liczba anulowanych
7. Liczba matogodzin anulowanych (**NOWA** — analogicznie, po „Liczba anulowanych")
8. Przychody spodziewane
9. Przychody na matę
10. Liczba mat (**NOWA** — obok „Przychody na matę")
11. Przychody anulowane
12. Przychody anulowane na matę
13. Zmiana liczbowa rezerwacji r/r
14. Zmiana % rezerwacji r/r
15. Zmiana liczbowa anulowanych r/r
16. Zmiana % anulowanych r/r

- Dla kolumn matogodzin (i matogodzin anulowanych) **nie** dodajemy zmian r/r.
- „Liczba mat" to liczba mat w danym mieście (obecnie dostępna w `mats_by_location`), używana też jako mianownik w „Przychody na matę".

### Źródło matogodzin

- Matogodziny są zwracane przez API `reservations_report` w polu `boardhours_taken` (na rekord rezerwacji). Należy je wyciągnąć w `rows_to_df` i agregować analogicznie do liczby rezerwacji.
- „Liczba matogodzin anulowanych" = suma `boardhours_taken` po rezerwacjach ze statusem anulowanym (analogicznie do „Liczba anulowanych").

### Podział na źródło w tabie Finanse

- Tab Finanse zachowuje podział na źródło rezerwacji — arkusze Sumarycznie / Online / Host / CC (jak w tabie Marketing). Zmienia się tylko układ wewnątrz arkusza (miasta jako kolumna zamiast osobnych tabel) oraz zestaw kolumn.

### Liczba mat

- „Liczba mat" to zawsze pojemność danego miasta (venue) z `mats_by_location` — wartość stała dla miasta, powtarzana w każdym wierszu danego miasta. Nie jest rozbijana per grupa atrakcji.

### Wiersze Suma

- Na dole płaskiej tabeli (w każdym arkuszu źródła) znajduje się wiersz „Suma", który sumuje wszystkie wiersze tabeli zgodnie z aktualnym podziałem — czyli łączna suma po miastach, dniach i grupach atrakcji widocznych w danym arkuszu.

### Zachowane

- Filtry (miasta, grupy atrakcji, checkbox „Rozdziel rodzaje atrakcji"), tryb (Miesiąc/Dzień), rodzaj daty (data stworzenia / odbycia) i porównania r/r rezerwacji/anulowanych działają jak dotychczas w obu tabach.

## Possible Edge Cases

- Brak danych matogodzin z API — kolumny matogodzin muszą pokazać wartość pustą/0 zamiast błędu.
- „Liczba mat" jest wartością per lokalizacja (pojemność venue), więc jej wartość powtarza się w każdym wierszu danego miasta (zamierzone).
- Dzień/miasto/atrakcja bez rezerwacji — czy wiersz ma się w ogóle pojawić w płaskiej tabeli (0), czy być pomijany.
- Podział na atrakcje wyłączony — tabela ma jeden wiersz na (Data × Miasto), bez kolumny „Rodzaj atrakcji".
- Rok poprzedni bez odpowiednika dnia (29.02) — r/r pozostaje puste, jak obecnie.
- Wiersz „Suma" na dole tabeli: agregacja całościowa po miastach/dniach/atrakcjach — unikać podwójnego liczenia; kolumny r/r i „Przychody na matę" liczone spójnie z sumą.

## Acceptance Criteria

- Strona raportu ma dwa taby: Finanse i Marketing, każdy z własnym generowaniem i pobieraniem `.xlsx`.
- Tab Marketing daje plik identyczny z dotychczasowym raportem.
- W tabie Finanse miasto jest kolumną; przy włączonym podziale na atrakcje jeden dzień daje do 45 wierszy.
- W tabie Finanse występują nowe kolumny: „Liczba matogodzin" (po „Liczba rezerwacji"), „Liczba matogodzin anulowanych" (po „Liczba anulowanych"), „Liczba mat" (obok „Przychody na matę").
- Kolumny matogodzin nie mają zmian r/r; pozostałe zmiany r/r działają jak dotychczas.
- Tab Finanse zachowuje arkusze wg źródła (Sumarycznie / Online / Host / CC).
- „Liczba mat" jest stała dla miasta i powtarza się w jego wierszach.
- Każdy arkusz w tabie Finanse ma wiersz „Suma" agregujący całą tabelę.
- Filtry i tryby działają w obu tabach.

## Open Questions

- Brak — wszystkie punkty rozstrzygnięte.
