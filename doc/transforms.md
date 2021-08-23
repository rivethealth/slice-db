# Built-in transforms

## Alphanumeric

Replace alphanumeric characters, preserving the type of characters.
Non-alphanumeric characters are left unchanged.

Class: `AlphanumericTranform`

Config:

- `unique` - Whether to generate a unique value

## City

Replace with US city.

Class: `CityTransform`

Uses
[plotly/datasets](https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv).

## Compose

Class: `ComposeTransform`

## Composite

Class: TODO

Parse as a PostgreSQL composite, with suboptions.

## Constant

Replace a non-null values with a string.

Class: `ConstantTransform`

Config: The value to use.

## Date (year)

Change date by up to one year.

Class: `DateYearTransform`

## Geozip

Replace zip code, preserving the first three digits.

Class: `GeopzipTransform`

Uses [simplemaps.com](https://simplemaps.com/data/us-zips).

Uses [www.ssa.gov](https://www.ssa.gov/cgi-bin/popularnames.cgi).

## Given name

Replace given name.

Class: `GivenNameTransform`

## JSONPath

Class: `JsonPathTransform`

Config: Array of entries where each entry is

- `path` - JSONPath expression
- `transform` - Name of transform

## Null

Null value.

Class: `NullTransform`

## Surname

Replace surname.

Class: `SurnameTransform`

Uses
[fivethirtyeight/data](https://raw.githubusercontent.com/fivethirtyeight/data/master/most-common-name/surnames.csv).

## US state

Class: `UsStateTransform`

Config:

- `abbr` - Whether to use abbreviation (default false)

Based on [rogerallen/1583593](https://gist.github.com/rogerallen/1583593)

## Words

Replace with random words.

Class `WordsTransform`

Uses
[first20hours/google-10000-english](https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-no-swears.txt).
