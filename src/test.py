from adaptor.binance import BinanceAlpha
from datetime import datetime as dt
import pandas as pd

def main():
    alpha = BinanceAlpha()
    print(dt.now())
    token_list = pd.DataFrame(alpha.get_token_list())
    print(dt.now())

    # get point multiplier > 1
    token_info = token_list.query("multiplier > 1").sort_values(by="volume_24h", ascending=False)

    target_num = 30
    for index, row in token_info.iloc[:target_num].iterrows():
        print(row['symbol'])

        symbol = f"{row['alpha_id']}USDT"
        print(dt.now())
        klines = alpha.get_klines(symbol, "15s", 240)
        print(dt.now())
        agg_trades = alpha.get_agg_trades(symbol, 500)
        print(dt.now())

if __name__ == "__main__":
    main()