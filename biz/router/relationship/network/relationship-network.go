// Code generated by hertz generator. DO NOT EDIT.

package network

import (
	"github.com/cloudwego/hertz/pkg/app/server"
	network "labelwall/biz/handler/relationship/network"
)

/*
 This file will register all the routes of the services in the master idl.
 And it will update automatically when you use the "update" command for the idl.
 So don't modify the contents of the file, or your code will be deleted when it is updated.
*/

// Register register routes based on the IDL 'api.${HTTP Method}' annotation.
func Register(r *server.Hertz) {

	root := r.Group("/", rootMw()...)
	{
		_api := root.Group("/api", _apiMw()...)
		{
			_v1 := _api.Group("/v1", _v1Mw()...)
			_v1.GET("/network", append(_getnetworkMw(), network.GetNetwork)...)
			_v1.POST("/nodes", append(_createnodeMw(), network.CreateNode)...)
			_nodes := _v1.Group("/nodes", _nodesMw()...)
			_nodes.DELETE("/:id", append(_deletenodeMw(), network.DeleteNode)...)
			_nodes.GET("/:id", append(_getnodeMw(), network.GetNode)...)
			_nodes.PUT("/:id", append(_updatenodeMw(), network.UpdateNode)...)
			{
				_node_id := _nodes.Group("/:node_id", _node_idMw()...)
				_node_id.GET("/relations", append(_getnoderelationsMw(), network.GetNodeRelations)...)
			}
			_v1.GET("/path", append(_getpathMw(), network.GetPath)...)
			_v1.POST("/relations", append(_createrelationMw(), network.CreateRelation)...)
			_relations := _v1.Group("/relations", _relationsMw()...)
			_relations.DELETE("/:id", append(_deleterelationMw(), network.DeleteRelation)...)
			_relations.GET("/:id", append(_getrelationMw(), network.GetRelation)...)
			_relations.PUT("/:id", append(_updaterelationMw(), network.UpdateRelation)...)
			{
				_nodes0 := _v1.Group("/nodes", _nodes0Mw()...)
				_nodes0.GET("/search", append(_searchnodesMw(), network.SearchNodes)...)
			}
		}
	}
}
