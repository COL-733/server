import tkinter as tk
from ring import Ring

class ServerGUI():

    def __init__(self, name, shutdown, exit, delete, add):
        self.delete_fn = delete
        self.add_fn = add
        self.r = tk.Tk()
        self.r.title(f"Server: {name}")
        self.r.geometry("350x400")

        serverLabel = tk.Label(self.r, text=f"Switch: {name.split('_')[0]}")
        serverLabel.grid(row=0, column=0, sticky=tk.W, pady=2)

        self.ringLabel = tk.Label(self.r, text=f"Ring:")
        self.ringLabel.grid(row=3, column=0, sticky=tk.W, pady=2)
        self.ringList = tk.Listbox(self.r, height=5, selectmode='single')
        self.ringList.grid(row=4, column=0, sticky=tk.W, pady=2)

        self.versionLabel = tk.Label(self.r, text=f"Versions:")
        self.versionLabel.grid(row=3, column=1, sticky=tk.W, pady=2)
        self.versionList = tk.Listbox(self.r, height=5, selectmode='single')
        self.versionList.grid(row=4, column=1, sticky=tk.NW, pady=2)

        failButton = tk.Button(self.r, text='Fail', width=10, command=exit)
        failButton.grid(row=0, column=1, padx=5, pady=2)

        shutdownButton = tk.Button(self.r, text='Shut Down', width=10, command=shutdown)
        shutdownButton.grid(row=1, column=1, padx=5, pady=2)

        deleteButton = tk.Button(self.r, text='Delete', width=10, command=self.delete)
        deleteButton.grid(row=5, column=0, pady=2)

        insertFrame = tk.Frame(self.r)
        insertFrame.grid(row=5, column=1)

        self.insertEntry = tk.Entry(insertFrame, width=8)
        self.insertEntry.grid(row=0, column=0, padx=5, pady=1, sticky=tk.W)

        insertButton = tk.Button(insertFrame, text='Insert', command=self.add)
        insertButton.grid(row=0, column=1, pady=1, sticky=tk.W)

        

    def mainloop(self):
        self.r.mainloop()
    
    def exit(self):
        self.r.destroy()
    
    def updateRing(self, ring: Ring):
        self.ringList.delete(0, tk.END)
        for i in range(len(ring.state)):
            v = ring.state[i]
            if v.server == ring.serverName:
                self.ringList.insert(i, "{0:20} {1}".format(v.server+"*", v.pos))
            else:
                self.ringList.insert(i, "{0:20} {1}".format(v.server, v.pos))
        
        self.versionList.delete(0, tk.END)
        i = 0
        for s, v in ring.versions.items():
            if s==ring.serverName:
                self.versionList.insert(i, "{0:20} {1}".format(s+"*", v))
            else:
                self.versionList.insert(i, "{0:20} {1}".format(s, v))
    
    def delete(self):
        try:
            pos = int(self.ringList.get(self.ringList.curselection()[0]).split()[-1])
            self.delete_fn(pos)
        except:
            pass
    
    def add(self):
        pos = self.insertEntry.get()
        try:
            pos = int(pos)
            self.add_fn(pos)
        except:
            pass
