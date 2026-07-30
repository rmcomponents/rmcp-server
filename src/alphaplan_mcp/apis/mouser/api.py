

# basic idea:
# POST https://api.mouser.com/api/v2/search/keyword?apiKey=YOUR_KEY
# apiKey from .env: MOUSER_API_KEY

{
  "SearchByKeywordRequest": {
    "keyword": "SOME_PART_NUMBER",
    "records": 50,
    "pageNumber": 1,
  }
}
