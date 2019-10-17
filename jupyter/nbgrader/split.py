import logging

from sklearn.model_selection import train_test_split

from jupyter import jloadl, jdumpl

logger = logging.getLogger(__name__)


def split_simple(dataset_file, dev_file, test_file, test_size, use_context=False):
    '''Split intio dev/test, if any test code in dev, move it to dev.'''
    logger.info('')

    # data = db.read_text([dataset_file]).map(json.loads).compute()
    data = jloadl(dataset_file)
    dev, test = train_test_split(data, test_size=test_size, random_state=0)
    logger.info('num in dev %s', len(dev))
    logger.info('num in test %s', len(test))

    # move target code of test into dev if it occurs in dev
    dev_code_test = set([tuple(js['code_tokens']) for js in dev])
    if use_context:
        # todo add all the context code to the set
        pass

    new_test = []
    for js in test:
        if not tuple(js['code_tokens']) in dev_code_test:
            new_test.append(js)
        else:
            dev.append(js)
    del test
    logger.info('num test after removing dev code in dev %s', len(new_test))


    logger.info('num in final dev %s %s', len(dev), dev_file)
    logger.info('num in final test %s %s', len(new_test), test_file)

    jdumpl(dev, dev_file)
    jdumpl(new_test, test_file)

