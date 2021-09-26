from interactionfreemongodb import App

app = App('config.ini', 'Storage', 'Asia/Shanghai')
app.start()
app.join()
