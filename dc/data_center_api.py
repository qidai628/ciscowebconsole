import requests
import json

# DC_HOST = "http://dc.futuredial.com/cmc"
DC_HOST = "http://54.169.119.156/cmc"

DC_USER = "cmc"
DC_PWD = "cmc1234!"

DC_USER_DELETE = "admin"
DC_PWD_DELETE = "fd123!"


def get_coll_name(cid, sid=None):
    if sid:
        return "cisco_whitelist_{}_{}".format(cid, sid)
    else:
        return "cisco_whitelist_{}".format(cid)


def get_all_collections(cid):
    all_colls = []
    colls = _get_collections(cid)
    for coll in colls:
        count = _get_document_count(coll["coll_name"])
        #if count > 0:
        coll["sn_count"] = count
        all_colls.append(coll)
    print(all_colls)
    return all_colls


def delete_collections(cid, coll_name_list):
    colls = _get_collections(cid)
    company_coll_names = [i["coll_name"] for i in colls]
    company_coll_map = {i["coll_name"]: i["etag"] for i in colls}
    for coll_name in coll_name_list:
        if coll_name in company_coll_names:
            _delete_collection(coll_name, company_coll_map[coll_name])


def get_all_documents(cid, sid=None):
    print("[get_all_documents]++ cid:{} sid:{}".format(cid, sid))
    uuid_list = []
    coll_name = get_coll_name(cid, sid)
    total_count = _get_document_count(coll_name)
    count = 0
    page = 1
    while count < total_count:
        uuid_page = _get_document_page(coll_name, page, 100)
        uuid_list.extend(uuid_page)
        count += len(uuid_page)
        page += 1
    print("[get_all_documents]-- coll_name:{} total_count:{} uuid_list_len:{}".format(coll_name, total_count, len(uuid_list)))
    print(uuid_list)
    return uuid_list 


def create_documents(uuid_list, cid, sid=None):
    coll_name = get_coll_name(cid, sid)
    colls = _get_collections(cid)
    company_coll_names = [i["coll_name"] for i in colls]
    if coll_name not in company_coll_names:
        _create_collection(cid, sid)
    for uuid in uuid_list:
        docs = _query_documents(coll_name, uuid)
        if not docs:
            _create_document(coll_name, uuid)


def delete_documents(uuid_list, cid, sid=None):
    coll_name = get_coll_name(cid, sid)
    for uuid in uuid_list:
        _delete_document(coll_name, uuid)


def _get_collections(cid):
    colls = []
    coll_name_company = get_coll_name(cid)
    coll_company = {}
    res = requests.get(url=DC_HOST, auth=(DC_USER, DC_PWD))
    if res and res.status_code==200:
        obj = res.json()
        if obj and obj.get("_returned", 0) > 0:
            e = obj.get("_embedded", {})
            if e and e.get("coll", {}):
                for c in e["coll"]:
                    if "_id" in c and "_etag" in c and "$oid" in c["_etag"]:
                        coll_name = c["_id"]
                        etag = c["_etag"]["$oid"]
                        if coll_name == coll_name_company:
                            coll_company = {"coll_name": coll_name, "cid":cid, "sid":None, "etag": etag}
                        else:
                            end = coll_name.rfind('_')
                            if end != -1 and coll_name[0:end] == coll_name_company:
                                sid = coll_name[end+1]
                                if sid.isdigit():
                                    sid = int(sid)
                                    colls.append({"coll_name": coll_name, "cid":cid, "sid": sid, "etag": etag})
    if coll_company:
        colls.insert(0, coll_company)
    return colls


def _create_collection(cid, sid=None):
    coll_name = get_coll_name(cid, sid)
    res = requests.put(url=DC_HOST+"/"+coll_name, auth=(DC_USER, DC_PWD),
                 headers={"Content-Type": "application/json"})
    return res


def _delete_collection(coll_name, etag):
    res = requests.delete(url=DC_HOST+"/"+coll_name, auth=(DC_USER_DELETE, DC_PWD_DELETE),
                 headers={"If-Match": etag})
    return res


def _get_document_count(coll_name):
    count = 0
    res = requests.get(url=DC_HOST+"/"+coll_name+"/?pagesize=0&count", auth=(DC_USER, DC_PWD), headers={"Content-Type": "application/json"})
    if res and res.status_code==200:
        obj = res.json()
        if obj and "_size" in obj:
            count = obj["_size"]
    print("[_get_document_count] coll_name:{} count:{}".format(coll_name, count))
    return count


def _get_document_page(coll_name, page, pagesize):
    uuid_list = []
    _returned = 0
    url = "{}/{}/?page={}&pagesize={}".format(DC_HOST, coll_name, page, pagesize)
    res = requests.get(url=url, auth=(DC_USER, DC_PWD), headers={"Content-Type": "application/json"})
    if res and res.status_code==200:
        obj = res.json()
        if obj and obj.get("_returned", 0) > 0:
            _returned = obj.get("_returned", 0)
            docs = obj.get("_embedded", {}).get("doc", [])
            for doc in docs:
                if "uuid" in doc:
                    uuid_list.append(doc["uuid"])
    print("[_get_document_page] coll_name:{} page:{} _returned:{} count:{}".format(coll_name, page, _returned, len(uuid_list)))
    return uuid_list 


def _create_document(coll_name, uuid):
    data = "{'uuid': '%s'}" % uuid
    res = requests.post(url=DC_HOST+"/"+coll_name, auth=(DC_USER, DC_PWD), data=data,
                         headers={"Content-Type": "application/json"})
    return res


def _query_documents(coll_name, uuid):
    docs = []
    filter = {'uuid': uuid}
    params = {'filter': json.dumps(filter)}
    res = requests.get(url=DC_HOST+"/"+coll_name, auth=(DC_USER, DC_PWD), params=params, headers={"Content-Type": "application/json"})
    if res and res.status_code==200:
        obj = res.json()
        if obj and obj.get("_returned", 0) > 0:
            docs = obj.get("_embedded", {}).get("doc", [])
    print(docs)
    return docs


def _delete_document(coll_name, uuid):
    docs = _query_documents(coll_name, uuid)
    for doc in docs:
        docid = doc.get('_id', {}).get('$oid', None)
        etag = doc.get('_etag', {}).get('$oid', None)
        if docid and etag:
            res = requests.delete(url=DC_HOST+"/"+coll_name+"/"+docid, auth=(DC_USER_DELETE, DC_PWD_DELETE),
                 headers={"If-Match": etag})


if __name__ == '__main__':
    #create_collection("cisco_whitelist_1_3")
   
    #create_document("cisco_whitelist_1_1", '33')
    #query_document("cisco_whitelist_1_1", '33')
    #get_all_collections(12)
    #delete_collection(12, ["cisco_whitelist_12_5", "cisco_whitelist_12_6", "cisco_whitelist_12_7"])
    #get_all_collections(12)
    #for i in range(10):
    #    _create_document("cisco_whitelist_1_1", '8888')
    #_query_document("cisco_whitelist_1_1", '8888')
    #get_all_collections(1)
    #delete_collection(1, ["cisco_whitelist_1_1"])
    # get_all_documents(1,1)
    # create_documents(['7777','9999'], 1, 1)

    # get_all_documents(1,2)
    #create_documents([str(i) for i in range(1, 120)], 1)
    get_all_collections(1)
    get_all_documents(1)
    #delete_documents(['1','2','3','4','5','119','120','121'], 1)
    # get_all_documents(1, 1)
    pass
    