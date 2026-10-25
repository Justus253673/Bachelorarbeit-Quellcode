# Ausführen: python ftPM.py --input ./aufzeichnungen --output ./ergebnisse

import argparse
import json
import os
import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL


def zeit(wert):  # standardisieren utc
    if not wert:
        return None
    return pd.to_datetime(wert, utc=True, format="ISO8601")


def nfc_save(aktion, zeitpunkt, modul, nfc_start, nfcount):
    zustand = aktion.get("state")
    if zustand == "RUNNING":
        if modul not in nfc_start:
            nfc_start[modul] = zeitpunkt
        return
    if zustand != "FINISHED" or not aktion.get("result"):
        return

    start = nfc_start.pop(modul, zeitpunkt)  # running=start, wenn fehlt dann end
    schluessel = (modul, aktion["result"], zeitpunkt)
    if schluessel not in nfcount:
        nfcount[schluessel] = {
            "serial": modul,
            "workpiece": aktion["result"],
            "start": min(start, zeitpunkt),
            "end": zeitpunkt
        }


def aktionsta_safe(aktion, zeitpunkt, aktionen):  # start ende separat, connect über ak-id
    nummer = aktion["id"]
    if nummer not in aktionen:
        aktionen[nummer] = {"start": None, "end": None, "result": None}
    eintrag = aktionen[nummer]

    zustand = aktion.get("state")
    if zustand == "RUNNING":
        if eintrag["start"] is None or zeitpunkt < eintrag["start"]:
            eintrag["start"] = zeitpunkt
        return

    if zustand not in ("FINISHED", "FAILED"):
        return

    if eintrag["end"] is None or zeitpunkt < eintrag["end"]:
        eintrag["end"] = zeitpunkt
    if aktion.get("result"):
        eintrag["result"] = aktion["result"]


def modstatus_read(topic, payload, aktionen, nfc_start, nfcount):
    zustaende = [payload.get("actionState")]  # einzeln
    zustaende += payload.get("actionStates") or []  # verlauf
    modul = topic.split("/")[-2]

    for aktion in zustaende:
        if not isinstance(aktion, dict) or not aktion.get("id"):
            continue

        zeitpunkt = zeit(aktion.get("timestamp") or payload.get("timestamp"))
        if aktion.get("command") == "RGB_NFC":
            nfc_save(aktion, zeitpunkt, modul, nfc_start, nfcount)
        else:
            aktionsta_safe(aktion, zeitpunkt, aktionen)


def json_test(zeile):
    try:  # topic + payload vorhanden?
        daten = json.loads(zeile)
        return daten["topic"], daten["payload"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def fertOrders_safe(payload, orders):
    for eintrag in payload:  # dedupl, später > früher
        if not isinstance(eintrag, dict):
            continue
        nummer = eintrag.get("orderId")
        if nummer:
            orders[nummer] = eintrag


def ftsOrd_safe(payload, ftschritte):  # fts zu order zuordnung, id: dock=prods
    for knoten in payload.get("nodes", []):
        aktion = knoten.get("action") or {}
        if aktion.get("type") == "DOCK":
            ftschritte[aktion["id"]] = payload.get("serialNumber")


def filterMsg(topic, payload, datendict):
    if topic == "ccu/order/completed" and isinstance(payload, list):
        fertOrders_safe(payload, datendict["orders"])
    elif isinstance(payload, dict) and topic.startswith("module/") and topic.endswith("/state"):
        modstatus_read(topic, payload, datendict["aktionen"], datendict["nfc_start"], datendict["nfcount"])
    elif isinstance(payload, dict) and topic.startswith("fts/") and topic.endswith("/order"):
        ftsOrd_safe(payload, datendict["ftschritte"])


def read_datei(pfad, datendict):
    anzahl = 0
    count = 0
    with open(pfad, encoding="utf-8") as datei:
        for zeile in datei:
            anzahl += 1
            msg = json_test(zeile)
            if msg is None:  # fehler zeilen überspringen
                continue
            count += 1
            topic, payload = msg
            filterMsg(topic, payload, datendict)
    return anzahl, count


def read_dateien(ordner):
    datendict = {  # dict übergreifend letztes > früher
        "orders": {},
        "aktionen": {},
        "ftschritte": {},
        "nfcount": {},
        "nfc_start": {}
    }

    for dateiname in sorted(os.listdir(ordner)):
        if not dateiname.lower().endswith(".json"):
            continue

        pfad = os.path.join(ordner, dateiname)
        anzahl, count = read_datei(pfad, datendict)
        if anzahl>0 and count == 0:
            print(f"Warnung:{dateiname} konnte nicht gelesen werden")
    return datendict["orders"], datendict["aktionen"], datendict["ftschritte"], list(datendict["nfcount"].values())


def Act_zeit(schritt, aktionen, ccu_start, ccu_ende):  # activity bilden
    act = f"{schritt['moduleType']} {schritt['command']}"
    ressource = schritt.get("serialNumber")
    start, ende, zeitquelle = ccu_start, ccu_ende, "ccu"
    ergebnis = None
    aktion = aktionen.get(schritt["id"])

    if aktion:  # aktionszeitstempel wenn vollständig; sonst ccu
        if aktion["start"] is not None and aktion["end"] is not None:
            start = aktion["start"]
            ende = aktion["end"]
            zeitquelle = "action"
        if schritt["command"] == "CHECK_QUALITY":
            ergebnis = aktion["result"]

    return act, ressource, start, ende, zeitquelle, ergebnis


def orderevent(order, schritt, beladen, aktionen, ftschritte):
    ccu_start = zeit(schritt["startedAt"])
    ccu_ende = zeit(schritt["stoppedAt"])

    if schritt["type"] == "NAVIGATION":
        art = "Transport" if beladen else "Navigation"
        act = f"{art} {schritt['source']} to {schritt['target']}"
        ressource = ftschritte.get(schritt["id"])
        start, ende, zeitquelle, ergebnis = ccu_start, ccu_ende, "ccu", None
    else:
        act, ressource, start, ende, zeitquelle, ergebnis = Act_zeit(
            schritt, aktionen, ccu_start, ccu_ende)
        if schritt["command"] == "DROP":
            beladen = True
        elif schritt["command"] == "PICK":
            beladen = False

    event = {  # event zuordnung
        "case:concept:name": order["orderId"],
        "concept:name": act,
        "start_timestamp": start,
        "time:timestamp": ende,
        "case:orderType": order.get("orderType"),
        "case:workpieceType": order.get("type"),
        "case:workpieceId": order.get("workpieceId"),
        "case:state": order.get("state"),
        "org:resource": ressource,
        "stepType": schritt["type"],
        "stepState": schritt["state"],
        "result": ergebnis,
        "stepId": schritt["id"],
        "timesource": zeitquelle,
        "ccuStart": ccu_start,
        "ccuEnd": ccu_ende,
        "_source": schritt.get("source"),
        "_target": schritt.get("target"),
        "_loaded": beladen if schritt["type"] == "NAVIGATION" else None
    }
    return event, beladen


def klassisches_log(orders, aktionen, ftschritte):  # order -> case
    events = []
    for order in orders.values():
        beladen = False
        for schritt in order.get("productionSteps", []):
            if schritt.get("state") == "CANCELLED":
                continue
            if not schritt.get("startedAt") or not schritt.get("stoppedAt"):
                continue

            event, beladen = orderevent(order, schritt, beladen, aktionen, ftschritte)
            events.append(event)

    log = pd.DataFrame(events)
    return log.sort_values(["case:concept:name", "time:timestamp"]).reset_index(drop=True)


def verbinden(beziehungen, event, objekt_id, objekttyp, rolle):
    beziehungen.append({
        "ocel:eid": event["ocel:eid"],
        "ocel:activity": event["ocel:activity"],
        "ocel:timestamp": event["ocel:timestamp"],
        "ocel:oid": objekt_id,
        "ocel:type": objekttyp,
        "ocel:qualifier": rolle
    })


def modulzuordnung(zeile, event, ws, fts_am_modul, objekte, beziehungen):
    verbinden(beziehungen, event, ws, "workpiece", "processes")
    modultyp = zeile["concept:name"].split()[0]
    befehl = zeile["concept:name"].split()[-1]
    modul_id = zeile["org:resource"]
    objekte[modul_id] = {"ocel:oid": modul_id, "ocel:type": modultyp}
    verbinden(beziehungen, event, modul_id, modultyp, "executed by")

    fts = fts_am_modul.get(modultyp)  # fts zusatz, letzte anfahrt
    if befehl not in ("PICK", "DROP") or pd.isna(fts):
        return
    rolle = "receives" if befehl == "DROP" else "delivers"
    verbinden(beziehungen, event, fts, f"fts {fts}", rolle)


def navzuordn(zeile, event, ws, fts_am_modul, objekte, beziehungen):
    event["source"] = zeile["_source"]
    event["target"] = zeile["_target"]
    event["loaded"] = zeile["_loaded"]
    fts = zeile["org:resource"]
    fts_am_modul[zeile["_target"]] = fts  # für pick + drop
    if zeile["_loaded"]:  # leer -> kein workpiece connect
        verbinden(beziehungen, event, ws, "workpiece", "transports")
    if pd.isna(fts):
        return

    fts_typ = f"fts {fts}"
    objekte[fts] = {"ocel:oid": fts, "ocel:type": fts_typ}
    verbinden(beziehungen, event, fts, fts_typ, "executed by")


def addNfc(events, objekte, beziehungen, nfcount, farben):
    for lesung in nfcount:
        ende = lesung["end"]
        ws = lesung["workpiece"]
        modul = lesung["serial"]
        zeittext = ende.strftime("%Y%m%dT%H%M%S%f")
        event_id = f"NFC_{modul}_{ws}_{zeittext}"
        event = {
            "ocel:eid": event_id,
            "ocel:activity": "DPS NFC READ",
            "ocel:timestamp": ende,
            "start_timestamp": lesung["start"],
            "stepState": "FINISHED", "result": None,
            "timesource": "action", "ccuStart": None, "ccuEnd": None,
            "source": None, "target": None, "loaded": None
        }
        if ws not in objekte:
            objekte[ws] = {
                "ocel:oid": ws, "ocel:type": "workpiece",
                "workpieceType": farben.get(ws)
            }
        if modul not in objekte:
            objekte[modul] = {"ocel:oid": modul, "ocel:type": "DPS"}
        verbinden(beziehungen, event, ws, "workpiece", "identifies")
        verbinden(beziehungen, event, modul, "DPS", "executed by")
        events.append(event)


def objektzentriertes_log(log, nfcount):
    events = []
    objekte = {}
    beziehungen = []
    o2o = []
    for order_id, schritte in log.groupby("case:concept:name"):
        erster = schritte.iloc[0]
        ws = erster["case:workpieceId"]
        objekte[order_id] = {
            "ocel:oid": order_id, "ocel:type": "order",
            "orderType": erster["case:orderType"], "state": erster["case:state"]
        }
        objekte[ws] = {
            "ocel:oid": ws, "ocel:type": "workpiece",
            "workpieceType": erster["case:workpieceType"]
        }
        o2o.append({"ocel:oid": order_id, "ocel:oid_2": ws, "ocel:qualifier": "contains"})
        fts_am_modul = {}
        for _, zeile in schritte.iterrows():
            event = {
                "ocel:eid": zeile["stepId"],
                "ocel:activity": zeile["concept:name"],
                "ocel:timestamp": zeile["time:timestamp"],
                "start_timestamp": zeile["start_timestamp"],
                "stepState": zeile["stepState"],
                "result": zeile["result"],
                "timesource": zeile["timesource"],
                "ccuStart": zeile["ccuStart"],
                "ccuEnd": zeile["ccuEnd"],
                "source": None, "target": None, "loaded": None
            }
            verbinden(beziehungen, event, order_id, "order", "belongs to")
            if zeile["stepType"] == "MANUFACTURE":
                modulzuordnung(zeile, event, ws, fts_am_modul, objekte, beziehungen)
            else:
                navzuordn(zeile, event, ws, fts_am_modul, objekte, beziehungen)
            events.append(event)

    farben = dict(zip(log["case:workpieceId"], log["case:workpieceType"]))
    addNfc(events, objekte, beziehungen, nfcount, farben)

    event_tabelle = pd.DataFrame(events)
    event_tabelle = event_tabelle.sort_values("ocel:timestamp").reset_index(drop=True)
   
    beziehungen.sort(key=lambda eintrag: eintrag["ocel:timestamp"])
    relationen = pd.DataFrame(beziehungen)

    return OCEL(events=event_tabelle,
                objects=pd.DataFrame(list(objekte.values())),
                relations=relationen,
                o2o=pd.DataFrame(o2o))


def statistiken(log, ordner):
  
    ord = []                #dict pro order
    for order_id, daten in log.groupby("case:concept:name"):
        ord.append({
            "case:concept:name": order_id,
            "orderType": daten["case:orderType"].iloc[0],
            "workpieceType": daten["case:workpieceType"].iloc[0],
            "state": daten["case:state"].iloc[0],
            "start": daten["start_timestamp"].min(),
            "ende": daten["time:timestamp"].max(),
            "events": daten["concept:name"].count(),
            "variante": " -> ".join(daten["concept:name"])
        })

    orders = pd.DataFrame(ord).set_index("case:concept:name")
    orders["durchlaufzeit_s"] = (orders["ende"] - orders["start"]).dt.total_seconds()
    qk_events = log[log["concept:name"] == "AIQS CHECK_QUALITY"]
    qk_start = qk_events.groupby("case:concept:name")["start_timestamp"].min()
    orders["zeit_bis_qk_s"] = (qk_start - orders["start"]).dt.total_seconds()
    orders.to_csv(os.path.join(ordner, "orders.csv"))
    
    gruppen = orders.groupby(["orderType", "workpieceType"])
    
    dlzeiten = gruppen["durchlaufzeit_s"].mean()
    qkzeiten = gruppen["zeit_bis_qk_s"].mean()
    kzz = []
    for (auftragstyp, farbe), daten in gruppen:
        kzz.append({
            "orderType": auftragstyp,
            "workpieceType": farbe,
            "Aufträge": daten["state"].count(),
            "Fehler": (daten["state"] == "ERROR").sum(),
            "Varianten": daten["variante"].nunique(),
            "durch.Durchlaufzeit_s": dlzeiten.loc[(auftragstyp, farbe)],
            "durch.Zeit_bis_QK_s": qkzeiten.loc[(auftragstyp, farbe)]
        })

    zahlen = pd.DataFrame(kzz).set_index(["orderType", "workpieceType"])
    nach_zustand = orders.pivot_table(
        index=["orderType", "workpieceType"],
        columns="state", values="zeit_bis_qk_s", aggfunc="mean"
    )
    for zustand in ("FINISHED", "ERROR"):
        if zustand in nach_zustand:
            zahlen[f"Zeit_bis_QK_{zustand}_s"] = nach_zustand[zustand]
    zahlen.to_csv(os.path.join(ordner, "kennzahlen.csv"))

    dauer = (log["time:timestamp"] - log["start_timestamp"]).dt.total_seconds()
    acts = dauer.groupby(log["concept:name"]).agg(["count", "mean", "min", "max"])
    acts = acts.rename(columns={
        "count": "Anzahl", "mean": "Dauer_Mittel_s", "min": "Dauer_Min_s", "max": "Dauer_Max_s"
    })
    acts.to_csv(os.path.join(ordner, "activities.csv"))


def klassische_modelle(log, ordner):
    teile = {"alle": log}
    
    for typ in log["case:orderType"].unique():
        teile[typ] = log[log["case:orderType"] == typ]

    produktion = log[log["case:orderType"] == "PRODUCTION"]
    for farbe in produktion["case:workpieceType"].dropna().unique():
        teile[f"PRODUCTION_{farbe}"] = produktion[produktion["case:workpieceType"] == farbe]
    
    for name, daten in teile.items():
        dfg, start, ende = pm4py.discover_dfg(daten)
        pm4py.save_vis_dfg(dfg, start, ende, os.path.join(ordner, f"dfg_häufigkeit_{name}.png"))
        dfg, start, ende = pm4py.discover_performance_dfg(daten)
        pm4py.save_vis_performance_dfg(dfg, start, ende, os.path.join(ordner, f"dfg_zeit_{name}.png"))
        netz, anfang, ende = pm4py.discover_petri_net_inductive(daten)
        pm4py.save_vis_petri_net(netz, anfang, ende, os.path.join(ordner, f"petri_netz_{name}.png"))
        bpmn = pm4py.discover_bpmn_inductive(daten)
        pm4py.save_vis_bpmn(bpmn, os.path.join(ordner, f"bpmn_{name}.png"))


def objektzentrierte_modelle(ocel, ordner, name):
    dfg = pm4py.discover_ocdfg(ocel)
    pm4py.save_vis_ocdfg(dfg, os.path.join(ordner, f"oc_dfg_häufigkeit_{name}.png"), annotation="frequency")
    pm4py.save_vis_ocdfg(dfg, os.path.join(ordner, f"oc_dfg_zeit_{name}.png"), annotation="performance")
    netz = pm4py.discover_oc_petri_net(ocel)
    pm4py.save_vis_ocpn(netz, os.path.join(ordner, f"oc_petri_netz_{name}.png"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="./aufzeichnungen")
    parser.add_argument("--output", default="./ergebnisse")
    args = parser.parse_args()
    ordner = {}
    for name in ("event_logs", "klassische_modelle", "objektzentrierte_modelle", "statistiken"):
        pfad = os.path.join(args.output, name)
        os.makedirs(pfad, exist_ok=True)
        ordner[name] = pfad

    orders, aktionen, ftschritte, nfc = read_dateien(args.input)
    if not orders:
        print("Keine abgeschlossenen Aufträge gefunden.")
        return

    log = klassisches_log(orders, aktionen, ftschritte)

    ocel = objektzentriertes_log(log, nfc)
    farben = dict(zip(log["case:workpieceId"], log["case:workpieceType"]))
    ocel_farben = {}
    for farbe in log["case:workpieceType"].dropna().unique():
        daten = log[log["case:workpieceType"] == farbe]
        passende_nfc = []
        for lesung in nfc:
            if farben.get(lesung["workpiece"]) == farbe:
                passende_nfc.append(lesung)
        ocel_farben[farbe] = objektzentriertes_log(daten, passende_nfc)

    log = log.drop(columns=["_source", "_target", "_loaded"])
    log.to_csv(os.path.join(ordner["event_logs"], "event_log.csv"), index=False)
    pm4py.write_xes(log, os.path.join(ordner["event_logs"], "event_log.xes"))
    pm4py.write_ocel2_sqlite(ocel, os.path.join(ordner["event_logs"], "ocel2.sqlite"))

    statistiken(log, ordner["statistiken"])
    klassische_modelle(log, ordner["klassische_modelle"])
    objektzentrierte_modelle(ocel, ordner["objektzentrierte_modelle"], "alle")
    for farbe, farb_log in ocel_farben.items():
        objektzentrierte_modelle(farb_log, ordner["objektzentrierte_modelle"], farbe)

    print(f"{log['case:concept:name'].nunique()} Aufträge, {len(log)} Events, "
          f"{len(ocel.events)} OCEL-Events, {len(ocel.objects)} Objekte")



if __name__ == "__main__":
    main()
