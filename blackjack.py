# -*- coding: UTF-8 -*-
"""=================================================
@IDE    ：PyCharm
@Author ：Strive Yang
@Date   ：2024-08-08 11:18
@Email  : yangzy927@qq.com
@Desc   ：一个关于21点的小游戏
=================================================="""
import random, sys

# 常量
HEARTS = chr(9829)  # ♥
DIAMONDS = chr(9830)  # ♦
SPADES = chr(9824)  # ♠
CLUBS = chr(9827)  # ♣

BACKSIDE = 'backside'


def main():
    print("""欢迎来玩21点
    规则：
    让你的牌总数尽量接近21
    JQK都是10点
    A可以为1点，也可以是11点
    2-10的点数和牌面一致
    输入h可以继续拿牌
    输入s停止拿牌
    两张牌时可以输入d加倍下注
    平局时赌注会回到玩家手中
    庄家总点数大于等于17点时会停止拿牌""")

    money = 5000
    while True:
        if money <= 0:
            print('--' * 50)
            print('您已破产！！！')
            print('--' * 50)
            sys.exit()

        # 输入赌注
        print(f'钱包余额为{money}')
        bet = get_bet(money)

        # 给庄家和玩家发牌
        deck = get_deck()
        dealer_hand = [deck.pop(), deck.pop()]
        player_hand = [deck.pop(), deck.pop()]

        # 处理用户操作
        print(f"赌注：{bet}")

        # 直到用户选择不再要排或者总点数超过21，打破循环
        while True:
            display_hands(player_hand, dealer_hand, False)
            print()

            # 检查是否总点数超过21点
            if get_hand_value(player_hand) > 21:
                break

            # 获得用户操作h,s,d
            move = get_move(player_hand, money - bet)

            if move == 'd':
                # 加注的范围是1到min(bet,余额)
                additional_bet = get_bet(min(bet, money - bet))
                bet += additional_bet
                print(f'赌注上涨到{bet}元')
                print(f'赌注：{bet}')

            if move in ('h', 'd'):
                new_card = deck.pop()
                rank, suit = new_card
                print(f"你得到了一张{suit}{rank}")
                player_hand.append(new_card)

                if get_hand_value(player_hand) > 21:
                    # 点数超过21
                    continue

            if move in ('s', 'd'):
                break

        # 庄家操作
        if get_hand_value(player_hand) <= 21:
            while get_hand_value(dealer_hand) < 17:
                # 庄家要牌
                print('庄家拿牌')
                dealer_hand.append(deck.pop())
                display_hands(player_hand, dealer_hand, False)

                # 庄家超过21
                if get_hand_value(dealer_hand) > 21:
                    break
                input('按回车键继续...')
                print('\n\n')

        # 展示最终结果
        display_hands(player_hand, dealer_hand, True)

        player_value = get_hand_value(player_hand)
        dealer_value = get_hand_value(dealer_hand)

        # 处理玩家结果，输赢和
        if dealer_value > 21:
            print(f'庄家爆牌，你赢了{bet}元')
            money += bet
        elif player_value > 21:
            print(f'你爆牌了，你输了{bet}元')
            money -= bet
        elif player_value < dealer_value:
            print(f'你的点数比庄家小，你输了{bet}元')
            money -= bet
        elif player_value > dealer_value:
            print(f'你的点数比庄家大，你赢了{bet}元')
            money += bet
        elif player_value == dealer_value:
            print("平局。赌注回到了你的钱包。")

        input("按回车键继续")
        print('\n\n')


def get_bet(max_bet):
    """问玩家下注"""
    # 直到输入有效金额才能退出循环
    while True:
        bet = input(f'请输入赌注(范围是1-{max_bet}，输入q推出)>>>').lower().strip()
        if bet == 'q':
            print('感谢使用，再会!')
            sys.exit()
        if bet.isdecimal():
            bet = int(bet)
            if 1 <= bet <= max_bet:
                return bet


def get_deck():
    """返回52张用元组表示的扑克牌，元组包含花色和数字"""
    deck = []
    for suit in (HEARTS, DIAMONDS, SPADES, CLUBS):
        for rank in range(2, 11):
            deck.append((str(rank), suit))
        for rank in 'JQKA':
            deck.append((rank, suit))

    # 洗牌
    random.shuffle(deck)
    return deck


def display_hands(player_hand, dealer_hand, show_dealer_hand):
    """展示手牌，False隐藏庄家第一张牌"""
    if show_dealer_hand:
        print(f'庄家: {get_hand_value(dealer_hand)}')
        display_cards(dealer_hand)
    else:
        print(f'庄家：？？？')
        # 隐藏第一张牌
        display_cards([BACKSIDE] + dealer_hand[1:])

    # 展示玩家手牌
    print(f'玩家: {get_hand_value(player_hand)}')
    display_cards(player_hand)

def get_hand_value(cards):
    """返回扑克牌的总点数"""
    value = 0
    # A的个数
    number_of_aces = 0

    # 除A以外的牌的点数加起来
    for card in cards:
        rank = card[0]
        if rank == 'A':
            number_of_aces += 1
        elif rank in 'JQK':
            value += 10
        else:
            value += int(rank)

    # 加上A
    # 1、按照1点加入总数
    value += number_of_aces
    for i in range(number_of_aces):
        if value + 10 <= 21:
            value += 10

    return value


def display_cards(cards):
    """展示所有牌"""
    rows = [''] * 5
    for card in cards:
        rows[0] += ' ___  '
        if card == BACKSIDE:
            rows[1] += '|## |  '
            rows[2] += '|###|  '
            rows[3] += '| ##|  '
        else:
            rank, suit = card
            rows[1] += f'|{rank:<2} | '
            rows[2] += f'| {suit} | '
            rows[3] += f'|_{rank:_>2}| '
    for row in rows:
        print(row)


def get_move(player_hand, money):
    """获取用户操作"""
    while True:
        moves = ['h-要牌', 's-停牌']
        if len(player_hand) == 2 and money > 0:
            moves.append('d-加倍')
        move_prompt = f"请输入你接下来的操作({','.join(moves)})"
        move = input(move_prompt)

        if move in ('h', 's'):
            return move

        if move == 'd' and 'd-加倍' in moves:
            return move


if __name__ == '__main__':
    main()
