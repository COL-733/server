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


class SwitchGUI():
    def __init__(self, switchName, add, rem):
        self.r = tk.Tk()
        self.add_fn = add
        self.rem_fn = rem
        
        self.r.title(f"Switch: {switchName}")

        addrLabel = tk.Label(self.r, text=f"Address of switch to connect:")
        addrLabel.grid(row=0, column=0, sticky=tk.W, pady=2)

        self.addrEntry = tk.Entry(self.r, width=8)
        self.addrEntry.grid(row=0, column=1, padx=2, pady=2, sticky=tk.W)
        
        nameLabel = tk.Label(self.r, text=f"Name of switch to connect:")
        nameLabel.grid(row=1, column=0, sticky=tk.W, pady=2)

        self.nameEntry = tk.Entry(self.r, width=8)
        self.nameEntry.grid(row=1, column=1, padx=2, pady=2, sticky=tk.W)

        addButton = tk.Button(self.r, text='Connect', width=10, command=self.addSwitch)
        addButton.grid(row=3, column=0, pady=2)        

        self.serverList = tk.Listbox(self.r, height=5, selectmode='single')
        self.serverList.grid(row=4, column=0, sticky=tk.W, pady=2)
        
        # removeButton = tk.Button(self.r, text='Connect', width=10, command=self.remSwitch)
        # removeButton.grid(row=3, column=0, pady=2)        

    def addSwitch(self):
        addr = self.addrEntry.get()
        name = self.nameEntry.get()
        self.add_fn(addr, name)

    def remSwitch(self):
        try:
            name = self.serverList.curselection()[0]
            self.rem_fn(name)
        except:
            pass

    def updateList(self, names):
        self.serverList.delete(0, tk.END)
        for i in range(len(names)):
            name = names[i]
            self.serverList.insert(i, name)

    def mainloop(self):
        self.r.mainloop()


class ClientGUI:
    def __init__(self, name, add, sync):
        self.r = tk.Tk()
        self.r.title(f"Client: {name}")
        self.add_fn = add
        self.sync_fn = sync

        self.addEntry = tk.Entry(self.r, width=8)
        self.addEntry.grid(row=0, column=0, padx=2, pady=2, sticky=tk.W)

        addButton = tk.Button(self.r, text='Add to Cart', width=10, command=self.addToCart)
        addButton.grid(row=0, column=1, pady=2)

        addLabel = tk.Label(self.r, text="My Cart:")
        addLabel.grid(row=1, column=0, sticky=tk.W, pady=2)

        self.itemsList = tk.Listbox(self.r, height=5, selectmode='multiple')
        self.itemsList.grid(row=2, column=0, sticky=tk.W, pady=2)
    
        syncButton = tk.Button(self.r, text='Sync Cart', width=10, command=self.syncCart)
        syncButton.grid(row=3, column=0, pady=2)        

    def addToCart(self):
        item = self.addEntry.get()
        self.add_fn([item])
    
    def syncCart(self):
        self.sync_fn()

    def updateList(self, cart):
        self.itemsList.delete(0, tk.END)
        for i in range(len(cart)):
            item = cart[i]
            self.itemsList.insert(i, item) 

    def mainloop(self):
        self.r.mainloop()
