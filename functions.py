def DCA_and_closed_trades(df):
    print(f'closed trade quantity: {df.RealizedProfit.nunique()}')
    print(f'total trade quantity: {df.RealizedProfit.count()}')

