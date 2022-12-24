import random

name_list = [
    'Saria',
    'Exusiai',
    'Skadi',
    'Chen',
    'Bagpipe',
    'Warfarin',
    'Spot',
    'SilverAsh',

]


def get_new_instance_name(old_names):
    '''
    蹲饼打工人值得一个名字。
    :param old_names: 已经起了的实例名字
    :return:
    '''
    name = 'SilverAsh'
    while name in old_names:
        name = random.choice(name_list)

    return name


if __name__ == '__main__':
    print(get_new_instance_name(old_names=['Saria', 'SilverAsh']))
