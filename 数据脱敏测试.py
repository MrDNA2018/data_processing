# 字母数字型，含车牌号、VIN码、邮编、固定电话、手机、身份证号
# 规则：3位之后增加1,6位置后增加A，9位之后增加2，12位会后增加B，15位之后增加3,18位之后增加C
# 如身份证号 430223198801010004，脱敏为：4301223 A1982801B0103004C


def en_type_char(old_str):
    if len(old_str) < 3:
        return old_str

    add_list = [
        '1',
        'A',
        '2',
        'B',
        '3',
        'C',
        '4',
        'D',
        '5',
        'E',
        '6',
        'F',
        '7',
        'G',
        '8',
        'H',
        '9',
        'I']
    new_str = ''
    for i in range(1, len(old_str) // 3 + 1):
        new_str += old_str[3 * (i - 1): 3 * i]
        new_str += add_list[i - 1]

    return new_str


old_str = '430223198801010004'
print(old_str)
print(en_type_char(old_str))
old_str = '粤S12382'
print(old_str)
print(en_type_char(old_str))
old_str = 'LZWACA12345678145'
print(old_str)
print(en_type_char(old_str))
old_str = '15111234887'
print(old_str)
print(en_type_char(old_str))
# %%
# 汉字型，含姓名、住址、出险地址、单位地址
# # 规则：姓名是两个字的中间加上“*”，姓名超过两个字的在姓前后加上各加上“*”和“#”
# # 如报案人陈卫国，脱敏为：*陈#卫国


def en_type_chinese_name(old_str):
    new_str = ''
    old_length = len(old_str)
    if old_length < 2:
        new_str = old_str
    if old_length == 2:
        new_str = old_str[0] + '*' + old_str[1]
    if old_length > 2:
        new_str = '*' + old_str[0] + '#' + old_str[1:]
    return new_str


old_str = '陈卫欧阳'
print(en_type_chinese_name(old_str))

# %%
# 汉字型，含姓名、住址、出险地址、单位地址
# # 规则：在省变更为东西，市变更为山水，区变更为河流，县变更为车辆
# # 如深圳市南山数字文化产业基地东塔楼708
# # 脱敏为：深圳山水南山数字文化产业基地东塔楼708


def en_type_chinese_addr(old_str):
    return old_str.replace(
        '省',
        '东西').replace(
        '市',
        '山水').replace(
            '区',
            '河流').replace(
                '县',
        '车辆')


old_str = '深圳市南山区数字文化产业基地东塔楼708'
print(en_type_chinese_addr(old_str))


# %%
# 报案号、立案号、结案号等
# 规则：将其中的年份减去100年
# 如：RDAA201844030000208842  脱敏为：RDAA191844030000208842


def en_type_registno(old_str):
    return old_str[:4] + str(int(old_str[4:6]) - 1) + old_str[6:]


old_str = 'RDAA201844030000208842'
print(en_type_registno(old_str))

# %%
# 查勘员姓名、定损员姓名、定损地点、修理厂名称等
# 规则：第一位调至倒数第一位，第二位调至倒数第二位，第三位调至倒数第三位
# 如香蜜湖定损中心    脱敏为：定损中心湖蜜香


def en_type_repairfactory(old_str):
    return old_str[3:] + old_str[2] + old_str[1] + old_str[0]


old_str = '香蜜湖定损中心'
print(en_type_repairfactory(old_str))
