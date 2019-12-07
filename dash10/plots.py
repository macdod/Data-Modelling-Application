import plotly.offline as po 
import plotly.graph_objs as go
import numpy as np
import pandas as pd
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvupdatedvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
def Sortdict(x,y): 
	d={}
	X=[]
	Y=[]
	for i,j in zip(x,y):
	    d[i]=j
	for z in sorted(d.keys()):
	    X.append(z)
	    Y.append(d[z])
	return X,Y



def call_function(x,y,fig_type,color,z='def'):
    if fig_type=='line':
        fig, file_name=Line_plot(x,y,color)
    if fig_type=='scatter':
        fig, file_name=Scatter_plot(x,y,color)
    if fig_type=='bar':
        fig, file_name=Bar_plot(x,y,color)
    return fig



def flip_axes(x,y):
    f.add_trace(call_function(x,y,fig_type,z='def'))
    f.plotly_restyle({'x':[y],'y':[x]},0)
    z = np.array(x)
    x = pd.Series(np.array(y))
    y = pd.Series(z)
    df = pd.concat([x, y])
    return df, f




def apply(x,y,fig_type,l_op,t_op,color='#fff',flip=False):
	f=go.FigureWidget()
	fig=call_function(x,y,fig_type,color='rgb(255,255,255)',z='def')
	f.add_trace(fig['data'][0])
	f.layout=fig['layout']
	x = pd.Series(np.array(x))
	y = pd.Series(np.array(y))
	df = pd.concat([x, y],axis=1)

	if flip==True:
		df, f = flip_axes(x, y)

	for l,op in zip(l_op,t_op):
		if l=='agg':
			if op=='mean':
			    f.plotly_restyle({'x':[list(df.groupby([0]).mean().index)],'y':[list(df.groupby([0]).mean()[1])]})
			if op=='max':
			    f.plotly_restyle({'x':[list(df.groupby([0]).max().index)],'y':[list(df.groupby([0]).max()[1])]})
			if op=='mode':
			    f.plotly_restyle({'x':[list(df.groupby([0]).mode().index)],'y':[list(df.groupby([0]).mode()[1])]})
			if op=='sum':
			    f.plotly_restyle({'x':[list(df.groupby([0]).sum().index)],'y':[list(df.groupby([0]).sum()[1])]})
			if op=='min':
			    f.plotly_restyle({'x':[list(df.groupby([0]).min().index)],'y':[list(df.groupby([0]).min()[1])]})
			if op=='stddev':
			    f.plotly_restyle({'x':[list(df.groupby([0]).std().index)],'y':[list(df.groupby([0]).std()[1])]})
			if op=='count':
			    f.plotly_restyle({'x':[list(df.groupby([0]).count().index)],'y':[list(df.groupby([0]).count()[1])]})
		elif l=='rng':
			f.plotly_relayout({'yaxis.range':[op[0],op[1]]})
	po.plot(f,auto_open=False,filename='dash10/templates/dash10/plots/plot1.html')
	return 'plot1.html'


#^^^^^^^updated^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

###### PREPROCESS ###########

def pre_process(x,y,z=[]):
	x,y,z=list(x),list(y),list(z)
	print("abcdbfhdj")
	print(x)
	print(y)
	if len(z)!=0:
		for i,j,k in zip(x,y,z):
			if i=='NULL':
				x[x.index(i)]=None
			if j=='NULL':
				y[y.index(j)]=None
			if k=='NULL':
				z[z.index(k)]=None
	else:
		print("else")
		for i,j in zip(x,y):
			if i=='NULL':
				x[x.index(i)]=None
			if j=='NULL':
				y[y.index(j)]=None
	print(x)
	print(y)
	return x,y,z


###### ADD LAYOUT FUNCTIONS ########
def ad_layout(n_x,n_y):
	layout=dict(
		xaxis=dict(
			visible=True,
			rangeslider=dict(
				visible=True,
				thickness=0.09
				),
			tickcolor="rgb(0,0,0)",
			title=dict(
				text=n_x,
				font=dict(size=20,color='rgb(0,0,0)')
				),
			tickfont=dict(color="rgb(0,0,0)")
		    ),
		yaxis=dict(
			visible=True,
			tickcolor="rgb(0,0,0)",
			fixedrange=False,
			title=dict(
				text=n_y,
				font=dict(size=20,color='rgb(0,0,0)')
				),
			tickfont=dict(color="rgb(0,0,0)")
			),
		plot_bgcolor='rgb(255, 255, 255)',
		paper_bgcolor='rgb(255, 255, 255)'
	)
	return layout

def ad_multi_layout(n_x,n_y):
	layout=dict(
		xaxis=dict(
			type='multicategory',
			visible=True,
			rangeslider=dict(
				visible=True,
				thickness=0.09
				),
			tickcolor="rgb(0,0,0)",
			title=dict(
				text=n_x,
				font=dict(size=20,color='rgb(0,0,0)')
				),
			tickfont=dict(color="rgb(0,0,0)")
			),
		yaxis=dict(
			visible=True,
			tickcolor="#fff",
			fixedrange=False,
			title=dict(
				text=n_y,
				font=dict(size=20,color='rgb(0,0,0)')
				),
			tickfont=dict(color="#fff")
			),
		plot_bgcolor='rgb(255, 255, 255)',
		paper_bgcolor='rgb(255, 255, 255)'
	)
	return layout





##### 3d_LAYOUT ###########
def ad_3d_layout(n_x,n_y,n_z):
	layout = dict(
		hoverlabel=dict(
				bgcolor='rgb(25,210,25,0.5)'
		),
		scene=dict(
			xaxis=dict(
				title=dict(
					text=n_x,
					font=dict(size=20,color='rgb(0,0,0)')
					),
				spikecolor='rgb(0,0,0)',
				gridcolor='rgb(0, 0, 0)',
				zerolinecolor='rgb(0, 0, 0)',
				showbackground=True,
				backgroundcolor='rgb(255,255,255)'
			),
			yaxis=dict(
				title=dict(
					text=n_y,
					font=dict(size=20,color='rgb(0,0,0)')
					),
				spikecolor='rgb(0,0,0)',
				gridcolor='rgb(0, 0, 0)',
				zerolinecolor='rgb(0, 0, 0)',
				showbackground=True,
				backgroundcolor='rgb(255,255,255)'
			),
			zaxis=dict(
				title=dict(
					text=n_z,
					font=dict(size=20,color='rgb(0,0,0)')
					),
				spikecolor='rgb(255,255,255)',
				gridcolor='rgb( 0, 0, 0)',
				zerolinecolor='rgb(0, 0, 0)',
				showbackground=True,
				backgroundcolor='rgb(255, 255, 255)'
			),
		
			camera=dict(
				up=dict(
					x=0,
					y=0,
					z=1
				),
				eye=dict(
					x=-1.7428,
					y=1.0707,
					z=0.7100,
				)
			),
			aspectratio = dict( x=1, y=1, z=0.7 ),
			aspectmode = 'manual'
		),
		plot_bgcolor='rgb(255, 255, 255)',
		paper_bgcolor='rgb(255, 255, 255)'
	)
	return layout


####### SETS THE CONFIGURATIONS##############
def set_configurations():
	configs = {
	'scrollZoom': True,
	'displayModeBar': True,
	'editable': True,
	'showLink':False,
	'displaylogo': False
	}
	return configs


####### LINE #####
def Line_plot(x,y,color = "#0000FF", c1="def", c2 = "def", selectedplot = 1,z = []):
	x,y,z=pre_process(x,y,z)
	if len(z)==0:
		X,Y=Sortdict(x,y)
		trace=go.Scatter(
			x=X,
			y=Y,
			line=dict(
				color=color,
				width=2,
				shape='spline',
				smoothing=1.1923,
				simplify=True
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_layout(c1,c2))
	else :
		Z,Y=Sortdict(z,y)
		trace=go.Scatter(
			x=[Z,x],
			y=Y,
			line=dict(
				color=color,
				width=2,
				shape='spline',
				smoothing=1.1923,
				simplify=True
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_multi_layout(c1,c2))

	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+selectedplot+'.html',config=configs)
	return fig,'plot1.html'



###### 3d_LINE ########
def Line_3d(x,y,z,c1,c2,c3,selectedplot=1):
	x,y,z=pre_process(x,y,z)
	trace = go.Scatter3d(
		x=x, y=y, z=z,
		marker=dict(
			size=5,
			color=[(int(str(float(i)).replace('.',''),10)%256) for i in z],
			colorscale='Viridis',
		),
		line=dict(
			color='#1f77b4',
			width=5
		)
	)
	data = [trace]
	fig=dict(data=data,layout=ad_3d_layout(c1,c2,c3))
	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+str(selectedplot)+'.html',config=configs)
	return fig,'plot1.html'




###### BAR ##########
def Bar_plot(x,y,color = "#0000FF", c1="def", c2 = "def",selectedplot = 1, z=[]):
	print(x)
	print(y)
	x,y,z=pre_process(x,y,z)
	print("------ >")
	print(x)
	print(y)
	if len(z)==0:
		trace=go.Bar(
		x=x,
		y=y,
		marker=dict(
			color=color,
			line=dict(
				color=color,
				width=5,
			),
			opacity=0.7
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_layout(c1,c2))
		print(fig)
	else :
		trace=go.Bar(
			x=[z,x],
			y=y,
			marker=dict(
				color=color,
				line=dict(
					color=color,
					width=5,
				),
				opacity=0.7
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_multi_layout(c1,c2))

	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+str(selectedplot)+'.html',config=configs)
	return fig,'plot1.html'





###### SCATTER ##########
def Scatter_plot(x,y,color = "#0000FF", c1="def", c2 = "def",selectedplot = 1, z=[]):
	x,y,z=pre_process(x,y,z)
	if len(z)==0 :
		trace=go.Scatter(
		x=x,
		y=y,
		mode='markers',
		marker=dict(
			    size=7,
				color=color
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_layout(c1,c2))

	else :
		trace=go.Scatter(
			x=[z,x],
			y=y,
			mode='markers',
			marker=dict(
				size=7,
				color=color
			)
		)
		data=[trace]
		fig=dict(data=data,layout=ad_multi_layout(c1,c2))

	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+selectedplot+'.html',config=configs)
	return fig,'plot1.html'





####### 3d_SCATTER ##########
def Scatter_3d(x,y,z,c1,c2,c3,selectedplot=1):
	trace1 = go.Scatter3d(
		x=x,
		y=y,
		z=z,
		mode='markers',
		marker=dict(
			size=4,
			color=z,
			colorscale='Viridis',
			opacity=0.8
		)
	)

	data = [trace1]
	fig = go.Figure(data=data, layout=ad_3d_layout(c1,c2,c3))
	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+selectedplot+'.html',config=configs)
	return fig,'plot1.html'







######## PIE #######
def Pie_chart(x,y,selectedplot = 1):
	trace=go.Pie(
		labels=x,
		values=y,
		hoverinfo='label+percent',textinfo='value',hole=0.2,textfont=dict(size=7),
		marker=dict(
			line=dict(
				color='rgb(255,9,87)',
				width=.7
			)
		)
	)
	data=[trace]
	layout=dict(
		plot_bgcolor='rgb(110, 110, 110)',
		paper_bgcolor='rgb(110, 110, 110)',
		)
	fig=dict(data=data,layout=layout)
	configs=set_configurations()
	po.plot(fig,auto_open=False,filename='dash10/templates/dash10/plots/plot'+selectedplot+'.html',config=configs)
	return fig,'plot1.htm'






###### TABLE #######
def Table(header,cells):
	trace=go.Table(
		header=dict(values=header),
		cells=dict(values=cells)
	)
	data=[trace]
	configs=set_configurations()
	po.plot(data,auto_open=False,filename='dash10/templates/dash10/plots/plot2.html',config=configs)
	return (data,'plot2.html')