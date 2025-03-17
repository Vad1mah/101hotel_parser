name: Run Daily Script

on:
  schedule:
    - cron: '30 2,17 * * *'
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11.9'
          cache: 'pip'

      - name: Install YDB CLI
        run: |
          curl -sSL https://install.ydb.tech/cli | bash
          echo "$HOME/ydb/bin" >> $GITHUB_PATH

      - name: Authenticate with YDB
        run: |
          echo '${{ secrets.AUTHORIZED_KEY_JSON }}' > authorized_key.json
          sudo apt-get update && sudo apt-get install -y jq
          export IAM_TOKEN=$(curl -s -d "@authorized_key.json" "https://iam.api.cloud.yandex.net/iam/v1/tokens" | jq -r .iamToken)
          echo "IAM_TOKEN=${IAM_TOKEN}" >> $GITHUB_ENV

      - name: Install Dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run Script
        env:
          CITIES: "huzhir,sahyurta,harantsi,sarma,kurma,buguldeyka,shida,agaluy,sayansk,slyudyanka,baikalsk,utulik,mangutay,kultuk,baykal,angarsk,savvateevka,leestvyanka,irkutsk,nikola,bolshoe_goloustnoe,granovshchina,hayryuzovka,bolshie_koti,patroni,ust-ilimsk"
          ROOMS_DATA_PATH: "./tables/rooms_data"
          HOTELS_STATISTICS_PATH: "./tables/hotels_statistics"
          DATABASE_FILE: "./databases/af_all_2024.csv"
          CITY_CENTER_AND_SEA_DISTANCES_FILE: "./databases/hotels_city_center_and_sea_distances.csv"
          TELEGRAM_TOKEN: ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          YDB_ENDPOINT: ${{ secrets.YDB_ENDPOINT }}
          YDB_DATABASE: ${{ secrets.YDB_DATABASE }}
          AUTHORIZED_KEY_PATH: "./authorized_key.json"
          LOG_FILE_PATH: "./logs/logging.log"
          IAM_TOKEN: ${{ env.IAM_TOKEN }}
        run: python main.py
