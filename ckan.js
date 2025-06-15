// ckan.js  (append or replace file)
export async function ckanCall(site, action, payload = {}, apiKey = "") {
  const opts = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(apiKey && { Authorization: apiKey }),
    },
    body: JSON.stringify({ ...payload }),
  };
  const res = await fetch(`${site}/api/3/action/${action}`, opts);
  const data = await res.json();
  if (!data.success) throw new Error(data.error?.message || "CKAN error");
  return data.result;
}

// ---------- 1-liners that mirror the Python helpers ----------
export const getFields = async (site, resId, key) =>
  (await ckanCall(site, "datastore_search", { resource_id: resId, limit: 0 }, key))
    .fields.map(f => f.id);

export const getSchema = async (site, resId, key) =>
  (await ckanCall(site, "datastore_search", { resource_id: resId, limit: 0 }, key)).fields;

export const getDataDictionary = getSchema; // CKAN returns same structure

export const setDataDictionary = async (site, resId, refFields, key) => {
  const fields = refFields[0]?.id === "_id" ? refFields.slice(1) : refFields;
  return ckanCall(
    site,
    "datastore_create",
    { resource_id: resId, fields, force: true },
    key
  );
};

// ---------- utilities ----------
export const schemaDict = schema =>
  Object.fromEntries(schema.map(({ id, type }) => [id, type]));

export async function compareSchemas(site, srcId, dstId, key) {
  const [src, dst] = await Promise.all([
    getSchema(site, srcId, key),
    getSchema(site, dstId, key),
  ]);
  const a = schemaDict(src);
  const b = schemaDict(dst);
  const sameNames = Object.keys(a).sort().join() === Object.keys(b).sort().join();
  const typeMismatches = Object.keys(a).filter(k => a[k] !== b[k]);
  return { sameNames, typeMismatches };
}

export async function cloneDataDictionary(site, srcId, dstId, key) {
  const ref = await getDataDictionary(site, srcId, key);
  return setDataDictionary(site, dstId, ref, key);
}
