Ext.onReady(function() {
	Ext.BLANK_IMAGE_URL = '../ext/resources/images/default/s.gif';

	Ext.QuickTips.init();

    //登陆
	function logon() {
		Ext.Msg.prompt('登陆窗口', '输入你的名字', function(btn, text) {
            //输入确定 并名字不为空
			if (btn == 'ok' && text.trim().length > 0) {
                //该用户的用户名 为全局变量 就一个
                username=text.trim();
				var tree = new Ext.tree.TreePanel({
					id : 'im-tree',
					title : 'Online Users',
					loader : new Ext.tree.TreeLoader(),
					rootVisible : false,
					lines : false,
					autoScroll : true,
					tools : [{
								id : 'refresh',
								on : {
									click : function() {
										var tree = Ext.getCmp('im-tree');
										tree.body.mask('Loading',
												'x-mask-loading');
										tree.root.reload();
										tree.root.collapse(true, false);
										setTimeout(function() { // mimic a
													// server
													// call
													tree.body.unmask();
													tree.root
															.expand(true, true);
												}, 1000);
									}
								}
							}],
					root : new Ext.tree.AsyncTreeNode({
								text : 'Online',
								children : []
							})
				});

				tree.on('dblclick', function(node) {
							showChatWindow(node);
						});
				var mainUI = new Ext.Window({
					id : 'acc-win',
					title : 'web IM -- ' + username,
					width : 250,
					height : 400,
					renderTo : document.body,
					iconCls : 'accordion',
					shim : false,
					animCollapse : false,
					constrainHeader : true,
					closable : false,
					draggable : true,
                    x:100,
                    y:100,
					tbar : [{
								tooltip : {
									title : 'Rich Tooltips',
									text : 'Let your users know what they can do!'
								},
								iconCls : 'connect',
								handler : function() {
								}
							}, '-', {
								tooltip : 'Add a new user',
								iconCls : 'user-add'
							}, ' ', {
								tooltip : 'Remove the selected user',
								iconCls : 'user-delete'
							}],

					layout : 'accordion',
					border : false,
					layoutConfig : {
						animate : false
					},

					items : [tree, {
								title : 'Settings',
								html : '<p>Something useful would be in here.</p>',
								autoScroll : true
							}, {
								title : 'Even More Stuff',
								html : '<p>Something useful would be in here.</p>'
							}, {
								title : 'My Stuff',
								html : '<p>Something useful would be in here.</p>'
							}]
				});
				mainUI.show();
                //开始与服务器通信
                joinListen();
			}else{
                //输入名字失败的话  再次打开登录窗口
                logon();
            }
		});

	}
	logon();
});
