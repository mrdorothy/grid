// Run test code on click

$('#js-test_code').click(function() {
	$.ajax({
            method: "POST",
            url: "/test_code",
            data: {}
        })
        .done(function(data) {
            $('#js-test_code_output').html(data)
        });
})