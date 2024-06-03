import sys
import time
import argparse
import requests
import settings


def print_error(message):
    print(message, file=sys.stderr)


def convert(dataset_doc):
    new_dataset_id = dataset_doc.get("instance_id")

    del dataset_doc["data_node"]
    del dataset_doc["index_node"]
    dataset_doc["id"] = new_dataset_id
    dataset_doc["access"] = {
        "https": {
            "name": "HTTPServer"
        },
        "globus": {
            "name": "Globus"
        }
    }

    facet_dict = {
        "root": "/css03_data"
    }
    for f in settings.facets:
        if f in dataset_doc:
            facet = dataset_doc.get(f)
            if isinstance(facet, list):
                facet_dict[f] = facet[0]
            else:
                facet_dict[f] = "v" + facet + "/"
    directory_string = dataset_doc.get(
        "directory_format_template_")[0] % facet_dict
    dataset_doc["location"] = {
        settings.location_tag: {
            "globus": settings.location_url + directory_string
        }
    }
    return dataset_doc


def convert2stac(dataset_doc):
    dataset_metadata = {}
    properties = {}

    property_keys = [
        "version",
        "access",
        "activity_drs",
        "activity_id",
        "cf_standard_name",
        "citation_url",
        #"data_node",
        "data_spec_version",
        "dataset_id_template_",
        #"datetime_start",
        #"datetime_stop",
        "directory_format_template_",
        "experiment_id",
        "experiment_title",
        "frequency",
        "further_info_url",
        #"north_degrees",
        #"west_degrees",
        #"south_degrees",
        #"east_degrees",
        #"geo",
        #"geo_units",
        "grid",
        "grid_label",
        #"height_bottom",
        #"height_top",
        #"height_units",
        #"index_node",
        #"instance_id",
        "institution_id",
        "latest",
        "master_id",
        "member_id",
        "metadata_format",
        "mip_era",
        "model_cohort",
        "nominal_resolution",
        #"number_of_aggregations",
        #"number_of_files",
        "pid",
        "product",
        "project",
        "realm",
        "replica",
        #"size",
        "source_id",
        "source_type",
        "sub_experiment_id",
        "table_id",
        #"title",
        #"type",
        #"url",
        "variable",
        "variable_id",
        "variable_long_name",
        "variable_units",
        "variant_label",
        #"xlink",
        #"_version_",
        "retracted",
        #"_timestamp",
        #"score",
    ]

    for k in property_keys:
        properties[k] = dataset_doc.get(k)
    properties["dataset_id"] = dataset_doc.get("instance_id")

    dataset_metadata["start_datetime"] = dataset_doc.get("datetime_start")
    dataset_metadata["end_datetime"] = dataset_doc.get("datetime_end")
    dataset_metadata["size"] = dataset_doc.get("size")
    dataset_metadata["title"] = dataset_doc.get("title")
    dataset_metadata["_version"] = dataset_doc.get("_version_")
    dataset_metadata["_score"] = dataset_doc.get("score")
    dataset_metadata["bbox"] = [
        dataset_doc.get("west_degrees"),
        dataset_doc.get("south_degrees"),
        dataset_doc.get("east_degrees"),
        dataset_doc.get("north_degrees"),
    ]
    dataset_metadata["collection"] = dataset_doc.get("project")[0].lower()
    dataset_metadata["stac_version"] = 1
    dataset_metadata["type"] = "item"

    #dataset_metadata["location"] = dataset_doc.get("")

    facet_dict = {
        "root": "/css03_data"
    }
    for f in settings.facets:
        if f in dataset_doc:
            facet = dataset_doc.get(f)
            if isinstance(facet, list):
                facet_dict[f] = facet[0]
            else:
                facet_dict[f] = "v" + facet + "/"
    directory_string = dataset_doc.get(
        "directory_format_template_")[0] % facet_dict

    dataset_metadata["links"] = [
        {
            "rel": "alternative",
            "type": "text/html",
            "href": settings.location_url + directory_string,
            "title": "ANL - Globus Webapp File Manager",
        }
    ]
    dataset_metadata["properties"] = properties
    return dataset_metadata


def get_esgf_response(path, document_type, offset, limit):
    # Search for datasets/files
    project = path.split("/")[0]
    params = {
        "limit": limit,
        "offset": offset,
        "project": project,
        "type": document_type,
    }

    # Add facets to query params
    if path:
        facets = path.split("/")
        for i, f in enumerate(facets):
            params[settings.facets[i]] = f

    # Add default query params
    params |= settings.default_query_params
    # print(params)

    r = None
    while r is None:
        try:
            r = requests.get(settings.esgf_search_url, params=params)
        except Exception as e:
            print_error("TimeoutError or other exception")
            time.sleep(30)
    
    if r.status_code != 200:
        print_error(f"The ESGF Index server returned {r.status_code}")
        sys.exit(1)

    try:
        search_response = r.json()
    except ValueError:
        print_error(
            "Error when decoding JSON response from the ESGF Index server"
        )
        sys.exit(1)
    # print(search_response)

    response = search_response.get("response")
    return response


def main(path):
    # Get the number of datasets in the path
    response = get_esgf_response(path, "Dataset", 0, 0)
    dataset_num_found = response.get("numFound", 0)
    if dataset_num_found == 0:
        print_error("No datasets found")
        sys.exit(1)
    print_error(f"{dataset_num_found} datasets in the path")

    # Get dataset documents in 1000 documents chunks
    chunk = 1000
    offset = 0
    new_datasets = 0
    while True:
        response = get_esgf_response(path, "Dataset", offset, chunk)
        new_dataset_num_found = response.get("numFound")
        # If the ESGF Search service is overloaded. Back off for 60 seconds and try again.
        if new_dataset_num_found != dataset_num_found:
            print_error(f"numFound when getting datasets: {new_dataset_num_found}")
            time.sleep(60)
            continue

        for doc in response.get("docs"):
            stac_doc = convert2stac(doc)
            settings.publish(stac_doc)

        offset += chunk
        if offset >= dataset_num_found:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Query the ESGF Index node and convert dataset/file documents into"
            " publish events to Apache Kafka"
        )
    )
    parser.add_argument(
        "--path",
        help=(
            "directory path to datasets, e.g."
            " 'CMIP6/AerChemMIP/AS-RCEC/TaiESM1/hist-piNTCF/r1i1p1f1'."
            " The path subdirecties are mapped to facets to create a query to"
            " the ESGF Search service"
        )
    )
    args = parser.parse_args()
    if args.path is None:
        parser.print_usage()
        sys.exit(1)

    main(args.path)
