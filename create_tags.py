
import csv
import os
import yaml

OUTPUT_TAGS_DIR = "meetyournextmsp-data-2021/tags"

with open('output/part-00000-403da5a3-fc33-4874-b47c-710cd6ab6274-c000.csv', newline='') as f:
    reader = csv.reader(f)
    headers = next(reader)
    for row in reader:
        region = row[1]
        constituency = row[2]

        region_id = region.replace(' ', '-').lower()
        constituency_id = constituency.replace(' ', '-').replace(',', '-').replace('--', '-').replace('--', '-').lower()


        if not os.path.exists(os.path.join(OUTPUT_TAGS_DIR, region_id, 'tag.yaml')):
            os.makedirs(os.path.join(OUTPUT_TAGS_DIR, region_id))
            with open(os.path.join(OUTPUT_TAGS_DIR, region_id, 'tag.yaml'), "w") as fp:
                fp.write(yaml.dump({'title': region, 'extra': {'region': True}}))

        if not os.path.exists(os.path.join(OUTPUT_TAGS_DIR, constituency_id, 'tag.yaml')):
            os.makedirs(os.path.join(OUTPUT_TAGS_DIR, constituency_id))
            with open(os.path.join(OUTPUT_TAGS_DIR, constituency_id, 'tag.yaml'), "w") as fp:
                fp.write(yaml.dump({'title': constituency, 'extra': {'constituency': True,'region':region_id}}))


