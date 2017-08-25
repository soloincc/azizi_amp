function BadiliDash() {
    this.data = {};
    this.theme = '';        // for jqWidgets
    this.console = console;
    this.currentView = undefined;
    this.csrftoken = $('meta[name=csrf-token]').attr('content');
    this.reEscape = new RegExp('(\\' + ['/', '.', '*', '+', '?', '|', '(', ')', '[', ']', '{', '}', '\\'].join('|\\') + ')', 'g')
    console.log('set csrf-token: '+ this.csrftoken)

    $.ajaxSetup({
      beforeSend: function(xhr, settings) {
        if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type)) {
            xhr.setRequestHeader("X-CSRFToken", dash.csrftoken)
        }
      }
   });
}

BadiliDash.prototype.initiate = function(){
    // initiate the creation of the interface
    this.initiateAllForms();
    this.initiateButtonsRadios();
};

/**
 * Creates a combobox with a list of all the forms
 * @returns {undefined}
 */
BadiliDash.prototype.initiateAllForms = function(){
    console.log(dash.data.all_forms);
    var source = {
        localdata: dash.data.all_forms,
        id:"id",
        datatype: "json",
        datafields:[ {name:"id"}, {name:"title"} ]
    };
    var data_source = new $.jqx.dataAdapter(source);
    $("#all_forms").jqxComboBox({ selectedIndex: 0, source: data_source, displayMember: "title", valueMember: "id", width: 300, theme: dash.theme });

    $('#all_forms').on('select', function(event){
        var args = event.args;
        if (args) {
            // get the form structure
            dash.formStructure(args.item.originalItem.uid);
        }
    });
};


BadiliDash.prototype.formStructure = function (form_id) {
    var data = {'form_id': form_id};

    $.ajax({
        type: "POST", url: "/form_structure/", dataType: 'json', data: data,
        error: dash.communicationError,
        success: function (data) {
            if (data.error) {
                Notification.show({create: true, hide: true, updateText: false, text: 'There was an error while communicating with the server', error: true});
                return;
            } else {
                console.log(data);
                dash.curFormStructure = data.structure;
                dash.initiateFormStructureTree();
            }
        }
    });
};


BadiliDash.prototype.initiateFormStructureTree = function () {
    var source ={
        datatype: "json",
        datafields: [
            { name: 'id' },
            { name: 'parent_id' },
            { name: 'name' },
            { name: 'type' },
            { name: 'label' }
        ],
        id: 'id',
        localdata: dash.curFormStructure
    };

    // create data adapter.
    var dataAdapter = new $.jqx.dataAdapter(source);
    // perform Data Binding.
    dataAdapter.dataBind();
    // get the tree items. The first parameter is the item's id. The second parameter is the parent item's id. The 'items' parameter represents
    // the sub items collection name. Each jqxTree item has a 'label' property, but in the JSON data, we have a 'text' field. The last parameter
    // specifies the mapping between the 'text' and 'label' fields.
    var records = dataAdapter.getRecordsHierarchy('id', 'parent_id', 'items', [{ name: 'name', map: 'value'}, {name: 'label', map: 'label'}]);
    $('#form_structure').jqxTree({ source: records, width: '95%', height: '550px', hasThreeStates: true, checkboxes: true});
};

BadiliDash.prototype.initiateButtonsRadios = function(){
    $("#get_data_btn").jqxButton({ template: "success" });
    $("#update_btn").jqxButton({ template: "info" });
    $("#populate_btn").jqxButton({ template: "primary" });
    $("#delete_btn").jqxButton({ template: "danger" });
    $(".action_btn").on('click', dash.processButtonAction );

    $("#destination .openspecimen").jqxRadioButton({ width: 250, height: 25});
    $("#destination .custom").jqxRadioButton({ width: 250, height: 25});
    $("#destination .bika_lims").jqxRadioButton({ width: 250, height: 25});
    $("#destination .other").jqxRadioButton({ width: 250, height: 25});
};

BadiliDash.prototype.processRadioAction = function(){};

BadiliDash.prototype.processButtonAction = function(event){
    var items = $('#form_structure').jqxTree('getCheckedItems');
    if(items === undefined){
        console.log('No forms defined...');
        swal({
          title: "Error!",
          text: "Please select at least one FORM to process.",
          imageUrl: "static/img/error-icon.jpg"
        });
        return;
    }
    if(items.length === 0){
        console.log('select nodes for processing...');
        swal({
          title: "Error!",
          text: "Please select at least one node for processing.",
          imageUrl: "static/img/error-icon.jpg"
        });
        return;
    }

    var node_ids = [];
    $.each(items, function(){
        node_ids[node_ids.length] = this.value;
    });
    var action = undefined, data = undefined, sel_form = $("#all_forms").jqxComboBox('getSelectedItem');

    switch(this.id){
        case 'get_data_btn':
            // ask whether to save the selected view or not
            swal({
              title: "Save View",
              text: "Do you want to save this view?<br /><p>Saving a view, <strong>generates a proper relational database</strong> from the submitted data. This allows user to run analysis on the saved data as well as process the submitted data</p>",
              type: "input",
              showCancelButton: true,
              closeOnConfirm: false,
              animation: "slide-from-top",
              inputPlaceholder: "View Name"
            },
            function(viewName){
              if (viewName === false) return false;
              
              if (viewName === "") {
                swal.showInputError("Please enter the name of the view!");
                return false
              }
              
            });
            $('#confirmModal').attr('aria-hidden', 'false');
            return;
            // download the data from the selected nodes
            dash.processDownloadButton(sel_form, data, node_ids, viewName);
        break;

        case 'update_btn':
            action = '/update_db_struct/';
        break;

        case 'populate_btn':
            action = 'populate_db';
        break;

        case 'delete_btn':
            action = '/delete_db/';
        break;
    };

    $.ajax({
        type: "POST", url: action, dataType: 'json', data: data,
        error: dash.communicationError,
        success: function (data) {
            if (data.error) {
                Notification.show({create: true, hide: true, updateText: false, text: 'There was an error while communicating with the server', error: true});
                return;
            } else {
                console.log(data);
            }
        }
    });
};

BadiliDash.prototype.processDownloadButton = function(sel_form, data, node_ids){
    var action = '/download/';
    var data = {'nodes[]': node_ids, 'form_id': sel_form.value, 'format': 'xlsx'};
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        console.log('onreadystatechange');
        var a;
        if (xhttp.readyState === 4 && xhttp.status === 200) {
            // Trick for making downloadable link
            a = document.createElement('a');
            console.log('Trick for making downloadable link');
            a.href = window.URL.createObjectURL(xhttp.response);
            // Give filename you wish to download
            var d = new Date();

            var datestring =
                  d.getFullYear() + ("0"+(d.getMonth()+1)).slice(-2) + ("0" + d.getDate()).slice(-2)
                  + "_" +
                  ("0" + d.getHours()).slice(-2) + ("0" + d.getMinutes()).slice(-2) + ("0" + d.getSeconds()).slice(-2);

            console.log(datestring);
            a.download = 'Form'+ sel_form.value + '_'+ datestring + '.xlsx';
            a.style.display = 'none';
            document.body.appendChild(a);
            console.log(a);
            a.click();
        }
    };
    // Post data to URL which handles post request
    xhttp.open("POST", action);
    xhttp.setRequestHeader("X-CSRFToken", dash.csrftoken);
    xhttp.setRequestHeader("Content-Type", "application/json");

    // You should set responseType as blob for binary responses
    xhttp.responseType = 'blob';
    xhttp.send(JSON.stringify(data));

    return;
};

BadiliDash.prototype.fnFormatResult = function (value, searchString) {
    var pattern = '(' + searchString.replace(dash.reEscape, '\\$1') + ')';
    return value.value.replace(new RegExp(pattern, 'gi'), '<strong>$1<\/strong>');
};

/**
 * Show a notification on the page
 *
 * @param   message     The message to be shown
 * @param   type        The type of message
 */
BadiliDash.prototype.showNotification = function(message, type, autoclose){
   if(type === undefined) { type = 'error'; }
   if(autoclose === undefined) { autoclose = true; }

   $('#messageNotification div').html(message);
   if($('#messageNotification').jqxNotification('width') === undefined){
      $('#messageNotification').jqxNotification({
         width: 350, position: 'top-right', opacity: 0.9,
         autoOpen: false, animationOpenDelay: 800, autoClose: autoclose, template: type
       });
   }
   else{ $('#messageNotification').jqxNotification({template: type}); }

   $('#messageNotification').jqxNotification('open');
};

/**
 * Initiates the Marsabit Dashboard
 *
 * @returns {undefined}
 */
BadiliDash.prototype.initiateMarsabitDashboard = function(){
    $("#refresh_database").on('click', dash.updateMarsabitDatabase );
};

BadiliDash.prototype.updateMarsabitDatabase = function(){
    var action = 'update_db';
    $.ajax({
        type: "POST", url: action, dataType: 'json',
        error: dash.communicationError,
        success: function (data) {
            if (data.error) {
                Notification.show({create: true, hide: true, updateText: false, text: 'There was an error while communicating with the server', error: true});
                return;
            } else {
                console.log(data);
            }
        }
    });
};

var dash = new BadiliDash();