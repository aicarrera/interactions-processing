import pandas as pd

# Set options to display all columns and rows without truncation
pd.set_option('display.max_columns', 7)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.max_rows', 70)
pd.set_option('display.expand_frame_repr', True)  # Prevent line wrapping of long rows
pd.set_option('display.width', 400)


def len_dic_sequences(dic_sequences):
    length = 0
    for k, v in dic_sequences.items():
        if not v["isInformative"]:
            length += 1
    return length


def create_intern_dict(dicIntern, index, event_name, date_timestamp, event_label, option, event_general=""):
    dicIntern["index"] = index
    dicIntern["event_name"] = event_name
    dicIntern["date_timestamp"] = date_timestamp
    dicIntern["event_label"] = event_label
    dicIntern["event_general"] = event_general
    dicIntern["option"] = option

    dicIntern["isInformative"] = False
    if dicIntern["event_name"].startswith("page") or dicIntern["event_name"] in ["scroll", "click", "form_submit"] or \
            dicIntern["event_name"].startswith("load") or dicIntern["event_name"].startswith("form_start") or dicIntern[
        "event_name"] == "user_engagement":
        dicIntern["isInformative"] = True
    event_label = event_label.replace(",", "_").strip("\"")
    if not dicIntern["isInformative"]:
        if event_label != "":
            dicIntern["unifiedEvent"] = "_".join(
                [event_general.split("_")[0], event_label.lower().replace(" ", "_"), event_general.split("_")[1]])
        else:
            dicIntern["unifiedEvent"] = "_".join([event_general.split("_")[0], event_general.split("_")[1]])


def getOption(event, last_option, inInnerSequence):
    option = last_option
    sideNavBarEvents = ["distributionOneChart_click", "distributionTwoCharts_click", "selectAction_change"]
    if not inInnerSequence:
        option = "dashboard_panel"

    if event.startswith("manualSelectionModal_click"):
        option = "manualSelection_modal"
        inInnerSequence = True
    elif event in sideNavBarEvents:
        option = "sidenavbar_panel"
        inInnerSequence = False
    elif event.startswith("selectIndicators"):
        option = "selectIndicators_modal"
        inInnerSequence = True
    elif event.startswith("selectMachine"):
        option = "machineSelection_main"
        inInnerSequence = False
    elif event.startswith("selectTransformations") or event.startswith("selectSignals"):
        option = "dashboard_panel"
        inInnerSequence = False
    elif event.startswith("closeSelectFilesModal") or event.startswith(
            "confirmFiles_click") or event.startswith("confirmParameters_click") or event.startswith(
        "closeParametersModal_click"):
        inInnerSequence = False

    return option, inInnerSequence


# Read interactions from platform
df = pd.read_csv("interactions_dataset/filtered_sessions15112023.csv")
df = df.drop(columns=['i'])

# Por el momento remuevo esto ya que es usuario d prueba.
df = df[df["user_id"] != "agonzalez"]
print(df.columns)

sequences = df.groupby(["ga_session_id", "user_id"]).agg({"date_timestamp": ["min", "max"], "event_name": ["count"]})
sequences = sequences.droplevel(0, axis=1).reset_index()
sequences["sequence"] = ""
sequencesMachine = df.groupby(["ga_session_id", "user_id", "machine", "page_location", "shift"]).agg(
    {"date_timestamp": ["min", "max"], "event_name": ["count"]})
sequencesMachine = sequencesMachine.droplevel(0, axis=1).reset_index()
print(sequencesMachine)

# Generating sequences machine
sequencesMachine["sequence"] = ""
sequential = 1
for i, row in sequencesMachine.iterrows():
    session_sequences = df[(df["ga_session_id"] == row["ga_session_id"]) & (df["machine"] == row["machine"]) & (
                df["page_location"] == row["page_location"])][
        ["event_name", "date_timestamp", "event_label", "page_location", "id", "shift"]]
    session_sequences = session_sequences.reset_index().sort_values(["id", "date_timestamp"],
                                                                    ascending=["True", "True"])
    # session_sequences=session_sequences.drop_duplicates(subset=["event_name","date_timestamp","event_label"])
    user_id = row["user_id"]
    shift = row["shift"]
    dicSequences = {}
    secIntern = False
    option = "timeSeries_visualizer"
    inInnerSequence = False
    for seq, row_int in session_sequences.iterrows():
        event = row_int["event_name"].replace(" ", "")
        timeStamp = row_int["date_timestamp"]

        if (row_int["event_name"].startswith("select") or "change" in row_int["event_name"]) and not pd.isna(
                row_int["event_label"]) and not row_int["event_name"].startswith("selectMachine"):
            try:
                dicEvent = eval(row_int["event_label"])
            except NameError:
                continue
            if len(dicEvent) != 0:
                for k in dicEvent:
                    if type(dicEvent[k]) == str and dicEvent[k] != "":
                        option, inInnerSequence = getOption(event, option, inInnerSequence)
                        create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"],
                                           row_int["event_name"], row_int["date_timestamp"], '"' + dicEvent[k] + '"',
                                           option, event[:event.index("_")] + "_change")

                    elif type(dicEvent[k]) == list and len(dicEvent[k]) > 0:
                        option, inInnerSequence = getOption(event, option, inInnerSequence)
                        for s in dicEvent[k]:
                            create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"],
                                               row_int["event_name"], row_int["date_timestamp"], '"' + s + '"', option,
                                               event[:event.index("_")] + "_change")
                            sequential += 1
                            # print("{},{},{},{},{},{}".format(timeStamp, machine, user_id, event[:event.index("_")], s,""))

        else:
            option, inInnerSequence = getOption(event, option, inInnerSequence)
            label = ""
            general = row_int["event_name"]
            if row_int["event_name"].startswith("selectMachine"):
                general = "selectMachine_click"
                label = row["machine"]
            elif row_int["event_name"].startswith("deleteChart"):
                general = "deleteChart_click"
            elif row_int["event_name"].startswith("loadChart"):
                general = "loadChart_click"

            create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"], row_int["event_name"],
                               row_int["date_timestamp"], label, option, general)

        sequential += 1

    sequencesMachine.at[i, "sequence"] = dicSequences

sequencesMachine.to_csv("sequencesMachine.csv")

file_processed_sequences = open("../graphdbMigrator/testIdeko/processedSequences.csv", "w")
file_lists_sequences = open("../graphdbMigrator/testIdeko/processedSequencesList.csv", "w")

dfProcessed = pd.read_csv("sequencesMachine.csv")

cabecera = "sequence_id,interaction_id,ga_session_id,user_id,machine,page_location,start_date,end_date,steps,shift,index_interaction,event_name, date_timestamp, event_label, event_general, option, isInformative, unifiedEvent \n"
cabecera_sequence = "sequence_id,ga_session_id,user_id,machine,page_location,start_date,end_date,steps,list_sequence\n"

file_processed_sequences.write(cabecera)
file_lists_sequences.write(cabecera_sequence)

for i, row in dfProcessed.iterrows():
    dicInternSequence = eval(row["sequence"])
    print(dicInternSequence, len_dic_sequences(dicInternSequence))
    if len_dic_sequences(dicInternSequence) < 2:
        continue
    print(
        "{},{},{},{},{},{},{},{},".format(i, row["ga_session_id"], row["user_id"], row["machine"], row["page_location"],
                                          row["min"], row["max"], len(dicInternSequence)))
    n = 1
    file_lists_sequences.write(
        "{},{},{},{},{},{},{},{},{},".format(i, row["ga_session_id"], row["user_id"], row["machine"],
                                             row["page_location"], row["min"], row["max"], len(dicInternSequence),
                                             row["shift"]))
    list_interactions = []
    for k, v in dicInternSequence.items():
        file_processed_sequences.write(
            "{},{},{},{},{},{},{},{},{},{},".format(i, n, row["ga_session_id"], row["user_id"], row["machine"],
                                                    row["page_location"], row["min"], row["max"],
                                                    len(dicInternSequence), row["shift"]))

        for ki, x in v.items():
            file_processed_sequences.write("{},".format(str(x)))
            if ki == "unifiedEvent" and v["isInformative"] == False:
                list_interactions.append(x)
        file_processed_sequences.write("\n")
        n += 1
    file_lists_sequences.write("\"{}\",\n".format(list_interactions))
file_processed_sequences.close()
