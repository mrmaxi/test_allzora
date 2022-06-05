import os
import xml2dict
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import namedtuple
from typing import List, Dict
import logging

log = logging.getLogger('compare')

tBrand = namedtuple('tBrand', ('name', 'source', 'items'))


def parse_size_amount(s: str) -> float:
    """parse size amount from size string
    >>>parse_size_amount('100 ml')
    100.0
    >>>parse_size_amount('100.0 ml')
    100.0
    >>>parse_size_amount('100,0 ml')
    100.0
    """

    if s:
        s = s.split(' ', 1)[0].replace(',', '.')
        if s.replace('.', '').isdigit():
            return float(s)


def parse_size_unit(s: str) -> str:
    """parse size unit of measurement from size string
    >>>parse_size_unit('100 ml')
    'ml'
    """

    if s and ' ' in s:
        return s.split(' ', 1)[1].strip()


def parse_size(s: str) -> dict:
    """parse size amount and unit of measurement from size string
    >>>parse_size('100')
    {'Weight': 100.0}
    >>>parse_size('100 ml')
    {'Weight': 100.0, 'Weight_UnitOfMeasurement': 'ml'}
    """
    rec = {}
    amount = parse_size_amount(s)
    if amount:
        rec['Weight'] = amount
    unit = parse_size_unit(s)
    if unit:
        rec['Weight_UnitOfMeasurement'] = unit
    return rec


def create_item(sour: str, rec: dict) -> dict:
    """
    unification name & brand:
    - clearing name from brand and weight
    - removing common words from brand
    """

    name = rec['name'].lower()

    weight = rec.get('Weight', '')
    measure = rec.get('Weight_UnitOfMeasurement', '')
    wm = ''
    if weight and isinstance(weight, (int, float)):
        for i in range(4):
            u = 3 - i
            wm = f'{weight:0.{u}f} {measure}'.strip()
            name = name.replace(wm, '')

        if round(weight) == round(weight, 3):
            weight = int(weight)
        wm = f'{weight} {measure}'.strip()

    brand = (rec.get('Brand') or '').lower()
    name = name.replace(brand, '')

    for common_word in ['professional', 'cosmetics', 'professionnel']:
        brand = brand.replace(common_word, '')

    name = name.replace(brand, '')

    if len(brand) <= 3:
        brand = 'other'

    return {
        'source_name': sour,
        'id': rec['id'],
        'ean_code': None,
        'brand': brand.strip(),
        'name': rec['name'],
        'wm': wm,
        'alias': name.strip(),
        'EANs': rec['EANs'],
        'raw':  rec
    }


def prepare_parsed_list(sour: str, recs: List[dict]) -> Dict[str, dict]:
    """create dict: ean code -> item, duplicate item record for each ean in EANs"""

    new_items = [create_item(sour, rec) for rec in recs]

    return {ean: {**item, 'ean_code': ean} for item in new_items for ean in item['EANs']}


def d1_parse(rec: dict) -> dict:
    return {
        **parse_size(rec.get('SIZE')),
        **rec,
        **rec.pop('WAREHOUSES', {}),
        'Brand': rec['MANUFACTURER'],
        'name': rec['NAME'],
        'EANs': [rec['EAN']] if 'EAN' in rec else [],
    }


def d2_parse(rec: dict) -> dict:
    return {
        **parse_size(rec.get('Weight')),
        **rec,
        **rec.pop('ProductTranslation', {}),
        'name': rec.get('name', rec.get('Description')),
        'EANs': [rec['EAN']] if 'EAN' in rec else [],
    }


def d3_parse(rec: dict) -> dict:
    return {
        **parse_size(rec.get('Contenido')),
        **rec,
        'Brand': rec['BrandName'],
        'id': rec['Id'],
    }


def add_source(sources: dict, fn: str, raw_recs: list):
    new_sour = os.path.basename(fn)
    new_items = prepare_parsed_list(new_sour, raw_recs)
    sources[new_sour] = new_items
    log.debug(f'{new_sour} parsed: {len(new_items)} items loaded')


def append_by_ean(ean_sources: dict, res_eans: dict, new_sour: str, new_items: dict):
    """
    find duplicated items by ean
    compare all items of new source with items of already combined sources

    :param ean_sources:     already combined sources - dicts of items by ean
    :param res_eans:        dict with connected items by ean
    :param new_sour:        new source name
    :param new_items:       items of new source - dict of items by ean
    """

    for ean, new_item in new_items.items():

        if ean in res_eans:
            bind = res_eans[ean]
            bind.append(new_item)
            new_item['bind'] = bind
            continue

        # Find by ean in rest records
        for sour, items in ean_sources.items():

            if ean in items:
                item0 = items[ean]

                bind = [item0, new_item]
                res_eans[ean] = bind
                item0['bind'] = bind
                new_item['bind'] = bind
                break

    ean_sources[new_sour] = new_items


def combine_by_ean(sources: dict) -> Dict[str, dict]:
    ean_sources = {}
    res_eans = {}

    for new_sour, new_items in sources.items():
        append_by_ean(ean_sources, res_eans, new_sour, new_items)

    log.debug(f'{len(res_eans)} items combined by ean')

    return res_eans


# list of brands with list of items
def group_items_by_brands(items: dict) -> dict:
    brands = {}
    for item in items.values():
        items = brands.setdefault(item['brand'], [])
        items.append(item)

    return brands


def create_brand_sources(sources: dict) -> dict:
    brand_sources = {}
    for new_sour, new_items in sources.items():
        brands = group_items_by_brands(new_items)
        brand_sources[new_sour] = brands
        log.debug(f'{new_sour} splited to {len(brands)} brands')

    return brand_sources


# Map of similar brands: brand name -> list of tBrand objects with items
# All similar brands have the same instance of list with each other
def match_brands(brand_sources: dict, similarity_threshold: int = 85) -> Dict[str, List[tBrand]]:
    """
    Create dict: brand name -> list of similar brands
    """

    res_brands = {}
    for new_sour, new_brands in brand_sources.items():
        for new_brand, new_items in new_brands.items():

            brand_obj = tBrand(new_brand, new_sour, list(new_items))

            # exact matching
            if new_brand in res_brands:
                res_brands[new_brand].append(brand_obj)
            elif res_brands:
                # Try to match brand by name similarity
                most_similar_brand, most_similar_score = \
                process.extractOne(new_brand, res_brands.keys(), scorer=fuzz.token_set_ratio)

                if most_similar_score >= similarity_threshold:
                    connected_list = res_brands[most_similar_brand]
                    connected_list.append(brand_obj)
                    res_brands[new_brand] = connected_list
                else:
                    res_brands[new_brand] = [brand_obj]
            else:
                res_brands[new_brand] = [brand_obj]

    return res_brands


def group_items_by_similar_brands(sources: dict, brand_similarity_threshold: int = 85) -> dict:
    """
    Create dict: brand name -> list of items of similar brands
    """

    # Create dict: source -> brand with its items
    brand_sources = create_brand_sources(sources)

    # combine similar brands together
    res_brands = match_brands(brand_sources, brand_similarity_threshold)
    log.debug(f'{len(res_brands)} unique brands found after matching with {brand_similarity_threshold}% threshold')

    # for each single brand create list with all items of similar brands
    res_brand_items = {
        brand: [rec for brand_obj in brand_list for rec in brand_obj.items]
        for brand, brand_list in res_brands.items()
    }
    log.debug(f'created item lists for {len(res_brand_items)} unique brands')

    return res_brand_items


def append_by_alias(res_brands_items: dict, res_arr: list, new_sour: str, new_items: dict, alias_similarity_threshold: int = 85):
    """
    Connect same items by similar brand+name+wm

    :param res_brands_items:            dict of brand items: brand -> all items of similar brands
    :param res_arr:                     result - list of connected items
    :param new_sour:                    name of processed source
    :param new_items:                   dict of processed items: ean -> item
    :param alias_similarity_threshold:  threshold of similarity to count items as the same
    """

    def same_source(item: dict) -> bool:
        return item['source_name'] == new_sour

    def not_bound_yet(item: dict) -> bool:
        if same_source(item):
            return False
        elif 'bind' in item:
            return not any(map(same_source, item['bind']))
        else:
            return True

    def get_alias(item: dict) -> str:
        return item['alias']

    for new_item in new_items.values():
        if 'bind' in new_item:
            continue

        alias = new_item['alias']
        brand = new_item['brand']
        wm = new_item['wm']
        brand_items = res_brands_items[brand]

        def check_item(item: dict) -> bool:
            return item['wm'] == wm and not_bound_yet(item)

        # possible duplicate items
        aliases = map(get_alias, filter(check_item, brand_items))

        # Find suitable brand in global brand list
        f = process.extractOne(alias, aliases, scorer=fuzz.token_set_ratio)
        if not f:
            continue

        most_similar_alias, most_similar_score = f
        if most_similar_score >= alias_similarity_threshold:
            item = None
            for item in filter(check_item, brand_items):
                if get_alias(item) == most_similar_alias:
                    break
            assert item, f"most_similar_alias={most_similar_alias} not found in filter(check_item, brand_items)"
            bind = item.setdefault('bind', [item])
            bind.append(new_item)

            if bind not in res_arr:
                res_arr.append(bind)


def combine_items_by_alias(sources: dict, res_brand_items: dict, alias_similarity_threshold: int = 85) -> list:
    """
    Process sources dict, connect same items by similar brand+name+wm

    :param sources:                     dict of source items: source name: items
    :param res_brand_items:             dict of brand items: brand -> all items of similar brands
    :param alias_similarity_threshold:  threshold of similarity to count items as the same
    :return:                            list of connected items
    """

    res_arr = []

    for new_sour, new_items in sources.items():
        append_by_alias(res_brand_items, res_arr, new_sour, new_items, alias_similarity_threshold)

    log.debug(f'{len(res_arr)} items combined by name with {alias_similarity_threshold}% threshold')
    return res_arr


def export_item(item: dict) -> dict:
    return {k: item[k] for k in ['source_name', 'id', 'ean_code', 'name']}


if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')

    sour_path = os.path.dirname(__file__)
    fn1 = os.path.join(sour_path, 'data_Soruce_1.xml')
    fn2 = os.path.join(sour_path, 'data_Source_2.xml')
    fn3 = os.path.join(sour_path, 'data_Source_3.json')
    res_fn = os.path.join(sour_path, 'Results.json')

    sources = {}

    # Parsing data_Soruce_1.xml
    l1 = xml2dict.parse(open(fn1, 'rb'))
    l1 = [d1_parse(rec) for rec in l1['SHOP']['SHOPITEM']]
    add_source(sources, fn1, l1)

    # Parsing data_Source_2.xml
    l2 = xml2dict.parse(open(fn2, 'rb'))
    l2 = [d2_parse(rec) for rec in l2['Stock']['Product']]
    add_source(sources, fn2, l2)

    # Parsing data_Source_3.json
    l3 = json.load(open(fn3, 'r', encoding='utf8'), strict=False)
    l3 = [d3_parse(rec) for rec in l3]
    add_source(sources, fn3, l3)

    # combine items by ean (exact)
    res_eans = combine_by_ean(sources)

    # combine items by brands
    res_brand_items = group_items_by_similar_brands(sources)

    # combine items by name (fuzzy)
    res_arr = combine_items_by_alias(sources, res_brand_items)

    # concatenate items combined by eans with items combined by name
    res_first = list(res_eans.values())
    res_second = [same_items for same_items in res_arr if same_items not in res_first]
    same_items_list = res_first + res_second

    # create output list with defined structure
    res = [[export_item(r) for r in same_items] for same_items in same_items_list]
    log.debug(f'{len(res)} items prepared as result list')

    json.dump(res, open(res_fn, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)
