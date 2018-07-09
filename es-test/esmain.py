import json


def main():
    with open("result.json") as result_json:
        result = json.load(result_json)
        print(result["hits"]["total"])


if __name__ == "__main__":
    main()
