from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance, Poloniex
from cryptofeed.defines import TICKER

import time
from qpython import qconnection


q = qconnection.QConnection(host='localhost', port=5005, pandas=True)
q.open()

q.sendAsync("""trades:([]systemtime:`datetime$();
        exchange:`symbol$();
        bid:`float$();
        ask:`float$();
        symbol:`symbol$())
        []time:`datetime$();""")


async def ticker(t, receipt_timestamp):
        localtime = time.time()

        print('Exchange: {} Symbol: {} System Timestamp: {} Bid: {} Ask: {} Time: {}'.format(
            t.exchange, t.symbol, localtime, t.bid, t.ask, t.timestamp
        ))
        q.sendAsync('`trades insert(.z.z;`{};{};{};`{})'.format(
            t.exchange,
            float(t.bid),
            float(t.ask),
            str(t.symbol),
            t.timestamp
        ))


ticker_cb = {TICKER: ticker}


def main():
    fh = FeedHandler()

    p_pairs = Poloniex.symbols()[:]
    fh.add_feed(Poloniex(symbols=p_pairs, channels=[TICKER], callbacks=ticker_cb))
    pairs = Binance.symbols()[:]
    fh.add_feed(Binance(symbols=pairs, channels=[TICKER], callbacks=ticker_cb))

    fh.run()


if __name__ == '__main__':
    main()
