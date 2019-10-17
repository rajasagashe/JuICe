import json
import logging

from tqdm import tqdm

from jupyter import get_files_under_dir

logger = logging.getLogger(__name__)

def dedup_dump(cells_indir, outfile_dir, key, key2=None):
    '''Dedups the records on the specified key and writes them out.'''
    logger.info('')
    seen = set()
    total = 0
    num_wo_key = 0
    num_wo_key2 = 0
    with open(outfile_dir+ f'/deduped_{key}.jsonl', 'w') as outfile:
        for path in tqdm(get_files_under_dir(cells_indir, '.jsonl')):
            for line in tqdm(open(path)):
                total += 1
                js = json.loads(line)
                if key not in js:
                    num_wo_key += 1
                    continue
                if js[key]:
                    if key2 and key2 in js and js[key2]:
                        val = tuple(js[key] + js[key2])
                    else:
                        num_wo_key2 += 1
                        val = tuple(js[key])

                    if val not in seen:
                        outfile.write(json.dumps(js)+'\n')
                        seen.add(val)

    logger.info('num wo key %s', num_wo_key)
    if key2:
        logger.info('num wo key %s', num_wo_key2)
    logger.info('Num input recs %s', total)
    logger.info(f'Num after deduping on %s %s', key, len(seen))

