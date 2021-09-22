import platform

if platform == 'Windows':
    cachepath = '\\cache'
    dfpath = '\\feathereddataframes'
    figurepath = '\\pickledfigures'
else:
    cachepath = '/cache'
    dfpath = '/feathereddataframes'
    figurepath = '/pickledfigures'

__version__ = '1.0.3'

if __name__ == '__main__':
    import rats.modules.RATS_CONFIG
    import rats.core.rats
