function ADGG() {
}

ADGG.prototype.initiateFarmersPage = function(){
    adgg.error_table = $('#farmers_list').dynatable({
      dataset: {
        paginate: true,
        recordCount: true,
        sorting: true,
        ajax: true,
        ajaxUrl: '/fetch_farmers_list/',
        ajaxOnLoad: true,
        records: []
      }
    });
};

var adgg = new ADGG();
