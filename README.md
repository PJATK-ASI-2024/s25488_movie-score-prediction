# Prognozowanie oceny filmu na podstawie danych z IMDB

## 1. Opis Projektu

### 1.1 Tło
Zestaw danych użyty w projekcie zawiera informacje o filmach, które obejmują różne atrybuty, takie jak nazwisko reżysera, czas trwania filmu, oceny krytyków, a także reakcje widzów, m.in. liczbę polubień na platformach społecznościowych. Projekt ma na celu zbadanie, które cechy filmu mają największy wpływ na ocenę na portalu IMDb i przewidywanie sukcesu filmu na podstawie jego atrybutów.

### 1.2 Opis atrybutów zestawu danych
Zestaw danych zawiera następujące atrybuty:

- **color**: Czy film jest w kolorze czy czarno-biały
- **director_name**: Nazwisko reżysera filmu
- **num_critic_for_reviews**: Liczba recenzji krytyków
- **duration**: Czas trwania filmu (w minutach)
- **director_facebook_likes**: Liczba polubień profilu reżysera na Facebooku
- **actor_3_facebook_likes**: Liczba polubień aktora nr 3 na Facebooku
- **actor_2_name**: Nazwisko aktora nr 2
- **actor_1_facebook_likes**: Liczba polubień aktora nr 1 na Facebooku
- **gross**: Dochód brutto filmu (w dolarach)
- **genres**: Gatunek filmu (np. 'Animation', 'Comedy', 'Romance')
- **actor_1_name**: Nazwisko aktora nr 1
- **movie_title**: Tytuł filmu
- **num_voted_users**: Liczba osób, które oceniły film
- **cast_total_facebook_likes**: Łączna liczba polubień całej obsady na Facebooku
- **actor_3_name**: Nazwisko aktora nr 3
- **facenumber_in_poster**: Liczba osób widocznych na plakacie filmowym
- **plot_keywords**: Słowa kluczowe opisujące fabułę
- **movie_imdb_link**: Link do filmu na IMDb (usunęliśmy ten atrybut w preprocessingu)
- **num_user_for_reviews**: Liczba recenzji użytkowników
- **language**: Język filmu
- **country**: Kraj produkcji
- **content_rating**: Kategoria wiekowa filmu
- **budget**: Budżet filmu (w dolarach)
- **title_year**: Rok premiery filmu
- **actor_2_facebook_likes**: Liczba polubień aktora nr 2 na Facebooku
- **imdb_score**: Ocena filmu na IMDb
- **aspect_ratio**: Proporcje obrazu filmu
- **movie_facebook_likes**: Łączna liczba polubień filmu na Facebooku

## 2. Wstępna analiza i podział danych

W ramach przygotowań do modelowania wykonano wstępną analizę oraz czyszczenie danych:

1. **Wczytanie danych**: Na początku wczytano zestaw danych zawierający informacje o filmach. Sprawdzono podstawowe informacje, takie jak początkowy kształt danych oraz typy kolumn, aby uzyskać przegląd struktury danych. Dane początkowo miały 5043 rekordów oraz 28 kolumn

2. **Czyszczenie danych**: W procesie czyszczenia usunięto kolumnę `movie_imdb_link`, która nie wnosiła istotnych informacji do analizy. Następnie usunięto wiersze zawierające brakujące wartości, co pomogło w zapewnieniu spójności i kompletności danych, które posłużą do modelowania. Po wyczyszczeniu danych zostało 3755 rekordów oraz 27 kolumn..

3. **Podział danych na zbiory treningowy i testowy**: Po zakończeniu wstępnej analizy podzielono dane na dwa zbiory – 70% do trenowania modelu oraz 30% do dalszego doszkalania. Podział ten pozwala na efektywne trenowanie modelu oraz późniejsze jego testowanie na odrębnych danych, co zwiększa wiarygodność wyników predykcji. Dane treningowe posiadają 2628 rekordy, a dane do doszkalnia 1127 rekordów.

