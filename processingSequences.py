import pandas as pd
import numpy as np
from collections import Counter
def create_intern_dict(dicIntern,index,event_name,date_timestamp,event_label,option,event_general=""):
    dicIntern["index"] = index
    dicIntern["event_name"] = event_name
    dicIntern["date_timestamp"] = date_timestamp
    dicIntern["event_label"] = event_label
    dicIntern["event_general"] = event_general
    dicIntern["option"] = option
    dicIntern["isInformative"] = False
    if dicIntern["event_name"].startswith("page") or dicIntern["event_name"].startswith("load") or dicIntern["event_name"].startswith("form_start") or dicIntern["event_name"]=="user_engagement":
        dicIntern["isInformative"] = True

# Set options to display all columns and rows without truncation
pd.set_option('display.max_columns', 6)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.max_rows', 70)
pd.set_option('display.expand_frame_repr', True)  # Prevent line wrapping of long rows
pd.set_option('display.width', 400)
df= pd.read_csv("interactions_dataset/filtered_sessions.csv")

df= df.drop(columns=['i'])
#Por el momento remuevo esto ya que es usuario d prueba.
df= df[df["user_id"]!= "agonzalez"]

print(df.columns)
sequences = df.groupby(["ga_session_id","user_id"]).agg({"date_timestamp":["min","max"],"event_name":["count"]})
sequences=sequences.droplevel(0,axis=1).reset_index()
sequences["sequence"]=""
sequencesMachine = df.groupby(["ga_session_id","user_id","machine","page_location"]).agg({"date_timestamp":["min","max"],"event_name":["count"]})
sequencesMachine= sequencesMachine.droplevel(0,axis=1).reset_index()
print(sequencesMachine)
#Generating sequence

for i, row in sequences.iterrows():
    session_sequences= df[df["ga_session_id"]==row["ga_session_id"]][["event_name","event_date","date_timestamp","machine","event_label","page_location","id"]]
    session_sequences=session_sequences.reset_index().sort_values(["id","date_timestamp"],ascending=["True", "True"])
    dicSequence= session_sequences.to_dict("index")
    sequences.at[i,"sequence"]=dicSequence

sequences.to_csv("sequences.csv")
sequencesMachine["sequence"]=""
#Generating sequences machine
print(df)
for i, row in sequencesMachine.iterrows():
    session_sequences= df[(df["ga_session_id"]==row["ga_session_id"])& (df["machine"]==row["machine"])][["event_name","date_timestamp","event_label","page_location","id"]]
    session_sequences=session_sequences.reset_index().sort_values(["id","date_timestamp"],ascending=["True", "True"])
    #d=session_sequences[(session_sequences["event_name"].str.startswith("select")) & (~session_sequences["event_label"].isna()) ]
    session_sequences=session_sequences.drop_duplicates(subset=["event_name","date_timestamp","event_label"])
    machine = row["machine"]
    user_id = row["user_id"]
    #print(session_sequences)
    dicSequences={}
    sequential = 1
    secIntern = False

    for seq, row_int in session_sequences.iterrows():
        event = row_int["event_name"]
        timeStamp = row_int["date_timestamp"]
        if event == "manualSelectionModal_click":
            option="manualSelectionModal"
            secIntern=True
        if event.startswith("selectMachine") or event.startswith("closeSelectFilesModal") or event.startswith("confirmFiles_click") or event.startswith("confirmParameters_click") or event.startswith("closeParametersModal_click"):
            secIntern = False

        if (row_int["event_name"].startswith("select") or "change" in row_int["event_name"])  and not pd.isna(row_int["event_label"])  and not row_int["event_name"].startswith("selectMachine"):
            try:
                dicEvent =eval(row_int["event_label"])
            except NameError:
                continue
            if len(dicEvent) != 0:
                for k in dicEvent:
                    if type(dicEvent[k]) == str and dicEvent[k] !="":
                        if event.startswith("selectIndicators"):
                            option = "selectIndicators"
                            secIntern = True
                        elif event.startswith("selectTransformations") or event.startswith("selectSignals"):
                            option = "timeseries_visualizer"
                            secIntern = False
                        create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"], row_int["event_name"], row_int["date_timestamp"], '"'+dicEvent[k]+'"',option, event[:event.index("_")]+"_change")
                    elif type(dicEvent[k]) == list and len(dicEvent[k])>0:
                        if event.startswith("selectIndicators"):
                            option = "selectIndicators"
                            secIntern = True
                        elif event.startswith("selectTransformations") or event.startswith("selectSignals"):
                            option = "timeseries_visualizer"
                            secIntern = False
                        for s in dicEvent[k]:
                            create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"], row_int["event_name"], row_int["date_timestamp"], '"'+s+'"',option, event[:event.index("_")]+"_change")
                            sequential += 1
                                #print("{},{},{},{},{},{}".format(timeStamp, machine, user_id, event[:event.index("_")], s,""))

        else:
            label=""
            general = row_int["event_name"]
            if row_int["event_name"].startswith("selectMachine"):
                option="machineSelection_main"
                general="selectMachine_click"
                label=row["machine"]
            elif row_int["event_name"].startswith("deleteChart"):
                general="deleteChart_click"
            elif row_int["event_name"].startswith("loadChart"):
                general = "loadChart_click"

            create_intern_dict(dicSequences.setdefault(sequential, {}), row_int["index"], row_int["event_name"], row_int["date_timestamp"], label, option, general)
            if not secIntern:
                option = "timeseries_visualizer"
        sequential+=1

    #print(dicSequences)
    #dicSequence = session_sequences.to_dict("index")
    sequencesMachine.at[i,"sequence"]=dicSequences

sequencesMachine.to_csv("sequencesMachine.csv")

d=open("../graphdbMigrator/testIdeko/processedSequences.csv", "w")
dfProcessed = pd.read_csv("sequencesMachine.csv")

cabecera= "seq,ga_session_id,user_id,machine,page_location,start_date,end_date,steps,index_seq, event_name, date_timestamp, event_label, event_general, option, isInformative \n"
d.write(cabecera)
for i, row in dfProcessed.iterrows():
    dicInternSequence= eval(row["sequence"])
    for k,v in dicInternSequence.items():
        d.write("{},{},{},{},{},{},{},{},".format(i,row["ga_session_id"], row["user_id"],row["machine"],row["page_location"],row["min"],row["max"],len(dicInternSequence)))
        l= list(v.values())
        for x in l:
            d.write("{},".format(x))
        d.write("\n")

d.close()


data_analysis=False
if data_analysis:
    print('Number of users: {}'.format(len(sequencesMachine.user_id.unique())))
    print(sequencesMachine.user_id.unique())
    print('Number of interaction sequences: {}'.format(len(sequencesMachine)))

    sequence_length = sequencesMachine.sequence.map(len).values

    print('Sequence length:\n\tAverage: {:.2f}\n\tMedian: {}\n\tMin: {}\n\tMax: {}'.format(
        sequence_length.mean(),
        np.quantile(sequence_length, 0.5),
        sequence_length.min(),
        sequence_length.max()))

    n_sessions_per_user = sequencesMachine.groupby('user_id').size()

    print('Sequences per user:\n\tAverage: {:.2f}\n\tMedian: {}\n\tMin: {}\n\tMax: {}'.format(
        n_sessions_per_user.mean(),
        np.quantile(n_sessions_per_user, 0.5),
        n_sessions_per_user.min(),
        n_sessions_per_user.max()))

    cnt = Counter()

    print('Most used machine: {}'.format(sequencesMachine.groupby("machine")["ga_session_id"].count()))
