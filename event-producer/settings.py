
esgf_search_url = "http://esgf-node.cels.anl.gov/esg-search/search"

default_query_params = {
    "data_node": "eagle.alcf.anl.gov",
    "format": "application/solr+json"
}

facets = [
    "mip_era",
    "activity_drs",
    "institution_id",
    "source_id",
    "experiment_id",
    "member_id",
    "table_id",
    "variable_id",
    "grid_label",
    "version"
]

location_tag = "ANL"
location_url = "https://app.globus.org/file-manager?origin_id=8896f38e-68d1-4708-bce4-b1b3a3405809&origin_path="

from producer import stdout as publish
