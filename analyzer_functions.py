def printTradeAnalysis(analyzer):
    '''
    Function to print the Technical Analysis results in a nice format.
    '''
    #Get the results we are interested in
    total_open = analyzer.total.open
    total_closed = analyzer.total.closed
    total_won = analyzer.won.total
    total_lost = analyzer.lost.total
    win_streak = analyzer.streak.won.longest
    lose_streak = analyzer.streak.lost.longest
    pnl_net = round(analyzer.pnl.net.total,2)
    strike_rate = (total_won / total_closed) * 100
    #Designate the rows
    h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost']
    h2 = ['Strike Rate','Win Streak', 'Losing Streak', 'PnL Net']
    r1 = [total_open, total_closed,total_won,total_lost]
    r2 = [strike_rate, win_streak, lose_streak, pnl_net]
    #Check which set of headers is the longest.
    if len(h1) > len(h2):
        header_length = len(h1)
    else:
        header_length = len(h2)
    #Print the rows
    print_list = [h1,r1,h2,r2]
    row_format ="{:<15}" * (header_length + 1)
    print("Trade Analysis Results:")
    for row in print_list:
        print(row_format.format('',*row))

    return print_list

def printSQN(analyzer):
    sqn = round(analyzer.sqn,2)
    print('SQN: {}'.format(sqn))
    return sqn

def printDrawDown(analyzer):

    drawdown = analyzer.drawdown
    moneydown = analyzer.moneydown
    len_drawdown = analyzer.len
    max_drawdown = analyzer.max.drawdown
    max_moneydown = analyzer.max.moneydown
    max_len_drawdown = analyzer.max.len
    
    # Designate the rows
    h1 = ['drawdown', 'moneydown', 'len_drawdown', 'max_drawdown','max_moneydown','max_len_drawdown']
    r1 = [drawdown, moneydown, len_drawdown, max_drawdown, max_moneydown, max_len_drawdown]
    # Print the rows
    print_list = [h1,r1]
    header_length = len(h1)
    row_format ="{:<15}" * (header_length + 1)
    print("Draw down Analysis Results:")
    for row in print_list:
        print(row_format.format('',*row))

    return print_list

def printSharpeR(analyzer):
    print("analyzer", analyzer)
    sharperatio = round(analyzer['sharperatio'],2)
    print('sharperatio: {}'.format(sharperatio))
    return sharperatio