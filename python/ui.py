# -*- coding=utf-8 -*-

from tkinter import *
import tkinter.filedialog as filedialog
from tkinter import ttk
import subprocess
import urllib.request
import json
import re


HOST = 'http://114.212.87.125:50070'
req = urllib.request

class WriteFrame(Frame):

	def choose_file(self):
		filename = filedialog.askopenfilename(initialdir = "/home/hadoop/",title = "choose your file",filetypes = (("all files","*.*"),))
		print(filename)
		self.path.set(filename)

	def refreshTree(self,event=None):
		self.tree = ttk.Treeview(self.subf2,show="tree")
		self.tree.insert("","end","/",text=".",tags=('click',))
		self.initializeTree(self.tree,"/")
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self.subf2,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

	def upload(self):
		self.exec_result.delete(1.0,END)
		p = subprocess.Popen("hadoop fs -put "+self.path.get()+" "+self.tree.focus(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
		#p = subprocess.Popen("/dir",stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
		stdout, stderr = p.communicate()
		#print(stdout)
		#print(stderr)
		self.exec_result.insert(END,stderr.decode())
		print(self.path.get())
		m = re.match(r".+/([^/]+)",self.path.get())
		print(m)
		filename = m.group(1)
		url = HOST+"/monitor?class=file&path="+self.tree.focus()+"/"+filename+"&key=blocks"
		blocks = json.loads(req.urlopen(url).read().decode())["block_id"]
		display=""
		for block in blocks:
			url = HOST+"/monitor?class=block&path="+self.tree.focus()+"/"+filename+"&blockID="+str(block)+"&key=replicas"
			replicas = json.loads(req.urlopen(url).read().decode())["replicas"]
			display += "数据块: "+str(block)+" \n\t存放位置: "+str(replicas)+"\n"
		self.exec_result.insert(END,display)
		self.refreshTree()

	def createWidgets(self):
		subf1 = Frame(self);
		self.select_file = Button(subf1)
		self.select_file["text"] = "选择上传文件"
		self.select_file["fg"]   = "red"
		self.select_file["command"] =  self.choose_file
		self.select_file.grid(row=0)

		self.path = StringVar()
		self.file_name = Entry(subf1, textvariable = self.path, width=50)
		self.file_name.grid(row=0,column=1,padx=5)

		self.upload_file = Button(subf1)
		self.upload_file["text"] = "确认上传"
		self.upload_file["fg"] = "red"
		self.upload_file["command"] = self.upload
		self.upload_file.grid(row=0,column=2,padx=5)

		subf1.grid(row=0)


		self.subf2 = Frame(self)
		self.tree = ttk.Treeview(self.subf2,show="tree")
		self.tree.insert("","end","/",text=".",tags=('refresh',))
		self.initializeTree(self.tree,"/")
		self.tree.tag_bind('refresh', '<1>', self.refreshTree)
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self.subf2,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

		self.exec_result = Text(self.subf2,height=17,width=60)
		self.exec_result.grid(row=0,column=2,columnspan=1, pady=5)
		self.subf2.grid(row=2)

	def display_selected(self,event):
		self.exec_result.delete(1.0, END)
		self.exec_result.insert(END, "上传至 "+self.tree.focus())

	def initializeTree(self, tree, path):
		url = HOST+"/monitor?class=file&path="+path+"&key=children"
		data = json.loads(req.urlopen(url).read().decode())
		print(data)
		if path[-1] != "/":
			path = path+"/"
		for node in data["child_local_name"]:
			url = HOST+"/monitor?class=file&path="+path+node+"&key=name"
			info = json.loads(req.urlopen(url).read().decode())
			print(info)
			if info["file_type"] == "directory":
				tree.insert(path[:-1],'end',info["full_name"],text=node,tags=('click',))
				self.initializeTree(self.tree, info["full_name"])
			else:
				#tree.insert(path[:-1],'end',info["full_name"],text=node)
				pass
		tree.tag_bind('click', '<1>', self.display_selected)

	def __init__(self, parent=None):
		Frame.__init__(self, parent)
		if parent != None: 
			if len([w for w in parent.winfo_children() if type(w)==WriteFrame]) > 1:
				return
		self.createWidgets()

class ReadFrame(Frame):
	def __init__(self, parent=None):
		Frame.__init__(self,parent)
		self.createWidgets()

	def download(self):
		self.exec_result.delete(1.0,END)
		p = subprocess.Popen("hadoop fs -get "+self.tree.focus()+" "+self.save_path.get(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
		stdout, stderr = p.communicate()
		#print(stdout)
		#print(stderr)
		self.exec_result.insert(END, stderr.decode())

	def refreshTree(self,event):
		self.tree = ttk.Treeview(self.subf2)
		self.tree.insert("","end","/",text=".",tags=('refresh',))
		self.tree.tag_bind('refresh', '<1>', self.refreshTree)
		self.initializeTree(self.tree,"/")
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self.subf2,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

	def createWidgets(self):
		subf1 = Frame(self)
		self.select_location = Button(subf1)
		self.select_location["text"] ="选择存放位置"
		self.select_location["fg"] = "red"
		self.select_location["command"] = self.choose_location
		self.select_location.grid(row=0)

		self.save_path = StringVar()
		self.save_locs = Entry(subf1, textvariable=self.save_path,width=50)
		self.save_locs.grid(row=0,column=1,sticky=W,padx=5)

		self.download_file = Button(subf1)
		self.download_file["text"] = "下载"
		self.download_file["fg"] = "red"
		self.download_file["command"] = self.download
		self.download_file.grid(row=0,column=2,sticky=W,padx=5)

		subf1.grid(row=0)

		self.subf2 = Frame(self)
		self.tree = ttk.Treeview(self.subf2)
		self.tree.insert("","end","/",text=".",tags=('refresh',))
		self.tree.tag_bind('refresh', '<1>', self.refreshTree)
		self.initializeTree(self.tree,"/")
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self.subf2,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

		self.exec_result = Text(self.subf2,height=17,width=60)
		self.exec_result.grid(row=0,column=2,columnspan=2,sticky=W, padx=5)

		self.subf2.grid(row=1)

	def choose_location(self):
		location = filedialog.askdirectory(initialdir = "/home/hadoop/",title = "choose your directory")
		self.save_path.set(location)

	def display_selected(self,event):
		self.exec_result.delete(1.0, END)
		self.exec_result.insert(END, "选择文件"+self.tree.focus())

	def initializeTree(self, tree, path):
		url = HOST+"/monitor?class=file&path="+path+"&key=children"
		data = json.loads(req.urlopen(url).read().decode())
		print(data)
		if path[-1] != "/":
			path = path+"/"
		for node in data["child_local_name"]:
			url = HOST+"/monitor?class=file&path="+path+node+"&key=name"
			info = json.loads(req.urlopen(url).read().decode())
			print(info)
			if info["file_type"] == "directory":
				tree.insert(path[:-1],'end',info["full_name"],text=node)
				self.initializeTree(self.tree, info["full_name"])
			else:
				tree.insert(path[:-1],'end',info["full_name"],text=node, tags=('click',))
		tree.tag_bind('click', '<1>', self.display_selected)

class ViewFrame(Frame):
	def __init__(self, parent=None):
		Frame.__init__(self,parent)
		self.createWidgets();

	def refreshTree(self,event):
		self.tree = ttk.Treeview(self)
		self.tree.insert("","end","/",text=".",tags=('refresh',))
		self.tree.tag_bind('refresh', '<1>', self.refreshTree)
		self.initializeTree(self.tree,"/")
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

	def createWidgets(self):
		self.tree = ttk.Treeview(self)
		self.tree.insert("","end","/",text=".",tags=('refresh',))
		self.tree.tag_bind('refresh', '<1>', self.refreshTree)
		self.initializeTree(self.tree,"/")
		self.tree.grid(row=0,sticky=NS)
		vbar = ttk.Scrollbar(self,orient=VERTICAL,command=self.tree.yview)
		self.tree.configure(yscrollcommand=vbar.set)
		vbar.grid(row=0,column=1,sticky=NS)

		self.selected = Text(self,height=20,width=80)
		self.selected.grid(row=0,column=2,padx=5,pady=5)

	def display_selected(self,event):
		try:
			self.selected.delete(1.0,END)
			url = HOST+"/monitor?class=file&path="+self.tree.focus()+"&key=storage"
			data = json.loads(req.urlopen(url).read().decode())
			display = "文件大小: "+str(data["file_size"])+"字节\n"+"数据块数量: "+str(data["block_count"])+"\n"
			url = HOST+"/monitor?class=file&path="+self.tree.focus()+"&key=matrix"
			matrix = json.loads(req.urlopen(url).read().decode())["coding_matrix"]
			display += "编码矩阵: \n"
			for row in matrix:
				display += "\t\t"+str(row)+"\n"
			url = HOST+"/monitor?class=file&path="+self.tree.focus()+"&key=blocks"
			blocks = json.loads(req.urlopen(url).read().decode())["block_id"]
			for block in blocks:
				url = HOST+"/monitor?class=block&path="+self.tree.focus()+"&blockID="+str(block)+"&key=replicas"
				replicas = json.loads(req.urlopen(url).read().decode())["replicas"]
				display += "数据块: "+str(block)+" \n\t存放位置: "+str(replicas)+"\n"
			self.selected.insert(END,display)
		except Exception:
			pass
		

	def initializeTree(self, tree, path):
		url = HOST+"/monitor?class=file&path="+path+"&key=children"
		data = json.loads(req.urlopen(url).read().decode())
		print(data)
		if path[-1] != "/":
			path = path+"/"
		for node in data["child_local_name"]:
			url = HOST+"/monitor?class=file&path="+path+node+"&key=name"
			info = json.loads(req.urlopen(url).read().decode())
			print(info)
			if info["file_type"] == "directory":
				tree.insert(path[:-1],'end',info["full_name"],text=node)
				self.initializeTree(self.tree, info["full_name"])
			else:
				tree.insert(path[:-1],'end',info["full_name"],text=node,tags=('click',))
		tree.tag_bind('click', '<1>', self.display_selected)
		# tree.insert('', 'end', 'widgets', text='Widget Tour',tags=('click','simple'))
 
		# # Same thing, but inserted as first child:
		# tree.insert('', 0, 'gallery', text='Applications',tags=('click','simple'))

		# # Treeview chooses the id:
		# id = tree.insert('', 'end', text='Tutorial',tags=('click','simple'))

		# # Inserted underneath an existing node:
		# tree.insert('gallery', 'end', text='Canvas',tags=('click','simple'))
		# tree.insert(id, 'end', text='Tree',tags=('click','simple'))

		# tree.tag_bind('click', '<1>', self.display_selected)

def switchFrame(option='Write'):
	cases = {"Write":wf, "Read":rf, "view":vf}
	for (k, v) in cases.items():
		v.pack_forget()
	cases[option].pack()

root = Tk()
root.title("Cumulus")
root.geometry("850x400")
wf = WriteFrame(parent=root)
rf = ReadFrame(parent=root)
vf = ViewFrame(parent=root)
menu = Menu(root)
menu.add_command(label='写文件', command=lambda : switchFrame(option="Write"))
menu.add_command(label='读文件', command=lambda : switchFrame(option="Read"))
menu.add_command(label='查看目录树', command=lambda : switchFrame(option="view"))
root["menu"] = menu
switchFrame("Write")
root.mainloop()
root.destroy()
