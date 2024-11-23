import tkinter as tk
from ring import Ring, VirtualNode

class ServerGUI():

    def __init__(self, name, shutdown):
        self.r = tk.Tk()
        self.r.title(f"Server: {name}")
        self.r.geometry("300x400")

        serverLabel = tk.Label(self.r, text=f"Switch: {name.split('_')[0]}")
        serverLabel.pack(padx=20, pady=5)

        self.ringLabel = tk.Label(self.r, text=f"Ring:")
        self.ringLabel.pack(padx=20, pady=5)
        self.ringList = tk.Listbox(self.r)
        self.ringList.pack(padx=20)

        button = tk.Button(self.r, text='Shut Down', width=25, command=shutdown)
        button.pack(pady=20)
    
    def mainloop(self):
        self.r.mainloop()
    
    def exit(self):
        self.r.destroy()
    
    def updateRing(self, ring: Ring):
        self.ringList.delete(0, tk.END)
        for i in range(len(ring.state)):
            v = ring.state[i]
            self.ringList.insert(i, "{0:20} {1}".format(v.server, v.pos))