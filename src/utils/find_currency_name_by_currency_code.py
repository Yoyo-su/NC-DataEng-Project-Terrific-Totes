def find_currency_name_by_currency_code(code):
    currency_codes_to_names = {
        "JPY": "Japanese yen",
        "BGN": "Bulgarian lev",
        "CZK": "Czech koruna",
        "DKK": "Danish krone",
        "GBP": "British pound",
        "HUF": "Hungarian forint",
        "PLN": "Polish zloty",
        "RON": "Romanian leu",
        "SEK": "Swedish krona",
        "CHF": "Swiss franc",
        "ISK": "Icelandic króna",
        "NOK": "Norwegian krone",
        "TRY": "Turkish new lira",
        "AUD": "Australian dollar",
        "BRL": "Brazilian real",
        "CAD": "Canadian dollar",
        "CNY": "Chinese/Yuan renminbi",
        "HKD": "Hong Kong dollar",
        "IDR": "Indonesian rupiah",
        "ILS": "Israeli new sheqel",
        "INR": "Indian rupee",
        "KRW": "South Korean won",
        "MXN": "Mexican peso",
        "MYR": "Malaysian ringgit",
        "NZD": "New Zealand dollar",
        "PHP": "Philippine peso",
        "SGD": "Singapore dollar",
        "THB": "Thai baht",
        "ZAR": "South African rand",
        "EUR": "European Euro",
        "USD": "United States dollar",
    }
    try:
        return currency_codes_to_names[code]
    except KeyError:
        raise KeyError("Currency code not found")
